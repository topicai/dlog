package dlog

import (
	"bytes"
	"crypto/md5"
	"encoding/gob"
	"encoding/hex"
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
	// TODO(y): Add writtenRecords, writtenBatches, failedRecords of type expvar.Int
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

	l := &Logger{
		Options:    opts,
		msgType:    t,
		streamName: n,
		buffer:     make(chan []byte),
		kinesis:    opts.kinesis(),
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
	if len(en) > maxMessageSize {
		return fmt.Errorf("Larger than 1MB Gob encoding of msg")
	} else {
		select {
		case l.buffer <- en:
		case <-timeout:
			// TODO(y): Add unit test for write timeout logic.
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
			buf = append(buf, encode(msg))
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
	if e != nil {
		// TODO(y): Retry sending the whole batch for up to n times.
		log.Printf("PutRecords error %v", e)
	} else if resp.FailedRecordCount > 0 {
		// TODO(y): Resend the failed records.
		log.Printf("PutRecords response error: %+v", resp)
	}

	*buf = (*buf)[0:0]
	*bufSize = 0
}

func partitionKey(data []byte) string {
	m := md5.Sum(data)
	return hex.EncodeToString(m[:])
}
