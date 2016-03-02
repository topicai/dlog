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

	// dlog syncs from buffered channels to Kinesis periodically.
	syncPeriod = time.Second
)

type Logger struct {
	*Options
	msgType    reflect.Type
	streamName string
	buffer     chan []byte
	kinesis    KinesisInterface
}

func NewLogger(example interface{}, opts *Options) (*Logger, error) {
	t, e := msgType(example)
	if e != nil {
		return nil, e
	}

	n, e := fullMsgTypeName(example)
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
	if !reflect.TypeOf(msg).AssignableTo(l.msgType) {
		return fmt.Errorf("parameter (%+v) not assignable to %v", msg, l.msgType)
	}

	var timeout <-chan time.Time // Receiving from nil channel blocks forever.
	if l.WriteTimeout > 0 {
		timeout = time.After(l.WriteTimeout)
	}

	en := encode(msg)
	if len(en) > maxMessageSize {
		log.Printf("Larger than 1MB Gob encoding of msg %+v", msg)
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
	ticker := time.NewTicker(syncPeriod)

	buf := make([][]byte, 0)
	bufSize := 0

	for {
		flush := false

		select {
		case msg := <-l.buffer:
			buf = append(buf, encode(msg))
			bufSize += len(msg)
			if bufSize > maxBatchSize {
				flush = true
			}

		case <-ticker.C:
			flush = true
		}

		if flush && bufSize > 0 {
			l.flush(buf)
			buf = buf[0:0]
			bufSize = 0
		}
	}
}

func (l *Logger) flush(buf [][]byte) {
	defer func() { // Recover if panicking
		if r := recover(); r != nil {
			log.Printf("Recover from error (%v)", r)
		}
	}()

	if len(buf) <= 0 {
		return
	}

	entries := make([]kinesis.PutRecordsRequestEntry, 0, len(buf))

	for _, msg := range buf {
		data, e := getMsgData(msg)
		if e != nil {
			continue
		}
		entries = append(entries, kinesis.PutRecordsRequestEntry{
			Data:         data,
			PartitionKey: getPartitionKey(data),
		})
	}

	resp, err := l.kinesis.PutRecords(l.streamName, entries)
	if err != nil {
		log.Printf("error happens when call PutRecords (%v)", err)
		return
	}

	log.Printf("success call PutRecords (%v)", resp)
	buf = buf[0:0]
}

func getMsgData(msg interface{}) ([]byte, error) {
	var buf bytes.Buffer

	e := gob.NewEncoder(&buf).Encode(msg)
	if e != nil {
		return nil, e
	}

	return buf.Bytes(), e
}

func getPartitionKey(data []byte) string {
	m := md5.Sum(data)
	return hex.EncodeToString(m[:])
}
