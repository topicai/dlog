package dlog

import (
	"bytes"
	"crypto/md5"
	"encoding/gob"
	"encoding/hex"
	"expvar"
	"fmt"
	"log"
	"reflect"
	"time"

	"github.com/AdRoll/goamz/kinesis"
	"github.com/topicai/candy"
)

const (
	// Maximum Kinesis message batch is no larger than 5MB.
	maxBatchSize = 5 * 1024 * 1024

	// Maximum Kinesis message size is 1MB.
	maxMessageSize = 1 * 1024 * 1024

	// We use MD5 to compute the partitionKey.
	partitionKeySize = 128
)

type Logger struct {
	*Options
	msgType    reflect.Type
	streamName string
	buffer     chan []byte
	kinesis    KinesisInterface

	// dlog exposed runtime metrics
	writtenRecords  *expvar.Int
	writtenBatches  *expvar.Int
	failedRecords   *expvar.Int
	tooBigMesssages *expvar.Int
}

func NewLogger(example interface{}, opts *Options) (*Logger, error) {
	t, e := msgType(example)
	if e != nil {
		return nil, e
	}

	n, e := opts.streamName(example)
	if e != nil {
		return nil, e
	}

	k, e := opts.kinesis()
	if e != nil {
		return nil, e
	}

	createdTime := time.Now().UnixNano()

	l := &Logger{
		Options:    opts,
		msgType:    t,
		streamName: n,
		buffer:     make(chan []byte),
		kinesis:    k,

		// use createdTime as name suffix to avoid conflict
		writtenRecords:  expvar.NewInt(fmt.Sprintf("%v--writtenRecords--%v", n, createdTime)),
		writtenBatches:  expvar.NewInt(fmt.Sprintf("%v--writtenBatches--%v", n, createdTime)),
		failedRecords:   expvar.NewInt(fmt.Sprintf("%v--failedRecords--%v", n, createdTime)),
		tooBigMesssages: expvar.NewInt(fmt.Sprintf("%v--tooBigMesssages--%v", n, createdTime)),
	}

	go l.sync()
	return l, nil
}

func (l *Logger) Log(msg interface{}) error {
	if t, e := msgType(msg); e != nil {
		return e
	} else if !t.AssignableTo(l.msgType) {
		return fmt.Errorf("parameter (%+v) not assignable to %v", msg, l.msgType)
	}

	var timeout <-chan time.Time // Receiving from nil channel blocks forever.
	if l.WriteTimeout > 0 {
		timeout = time.After(l.WriteTimeout)
	}

	en := encode(msg)
	if (len(en) + partitionKeySize) > maxMessageSize {
		l.tooBigMesssages.Add(1)
		return fmt.Errorf("Size of gob-encoded message plus partition key larger than %d bytes", maxMessageSize)
	} else {
		select {
		case l.buffer <- en:
		case <-timeout:
			return fmt.Errorf("dlog writes %+v timeout after %v", msg, l.WriteTimeout)
		}
	}
	return nil
}

func encode(v interface{}) []byte {
	var buf bytes.Buffer
	candy.Must(gob.NewEncoder(&buf).Encode(v)) // Very rare case of errors.
	return buf.Bytes()
}

func (l *Logger) sync() {
	if l.SyncPeriod <= 0 {
		l.SyncPeriod = time.Second
	}
	ticker := time.NewTicker(l.SyncPeriod)

	buf := make([][]byte, 0)
	bufSize := 0

	for {
		select {
		case msg := <-l.buffer:
			if bufSize+len(msg)+partitionKeySize >= maxBatchSize {
				l.flush(&buf, &bufSize)
			}

			buf = append(buf, msg)
			bufSize += len(msg) + partitionKeySize

		case <-ticker.C:
			if bufSize > 0 {
				l.flush(&buf, &bufSize)
			}
		}
	}
}

func (l *Logger) flush(buf *[][]byte, bufSize *int) {

	entries := make([]kinesis.PutRecordsRequestEntry, 0, len(*buf))
	for _, msg := range *buf {
		entries = append(entries, kinesis.PutRecordsRequestEntry{
			Data:         msg,
			PartitionKey: partitionKey(msg),
		})
	}

	resp, e := l.kinesis.PutRecords(l.streamName, entries)
	if e == nil {
		if resp.FailedRecordCount > 0 {
			log.Printf("PutRecords some records failed: %+v", resp)
		}

		l.writtenBatches.Add(1)
		l.writtenRecords.Add(int64(len(entries) - resp.FailedRecordCount))
		l.failedRecords.Add(int64(resp.FailedRecordCount))

	} else {
		log.Printf("PutRecords failed: %v", e)

		l.failedRecords.Add(int64(len(entries)))
	}

	// reset buf and bufSize
	*buf = (*buf)[0:0]
	*bufSize = 0
}

func partitionKey(data []byte) string {
	m := md5.Sum(data)
	return hex.EncodeToString(m[:])
}
