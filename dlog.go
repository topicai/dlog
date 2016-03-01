package dlog

import (
	"fmt"
	"reflect"
	"time"
)

const (
	// Message batches acceptable by Kinesis is no larger than 1MB.
	maxBatchSize = uintptr(1024 * 1024)

	// dlog writes into buffered channels. Here is the write timeout.
	writeTimeout = time.Second * 10

	// dlog syncs from buffered channels to Kinesis periodically.
	syncPeriod = time.Second
)

type Logger struct {
	msgType    reflect.Type
	streamName string
	batchSize  int
	buffer     chan interface{}
}

func NewLogger(example interface{}) (*Logger, error) {
	t := reflect.TypeOf(example)

	n, e := fullMsgTypeName(t)
	if e != nil {
		return nil, e
	}

	b, e := batchSize(t)
	if e != nil {
		return nil, e
	}

	// New messages may come during flushing.
	buf := make(chan interface{}, 2*b)

	l := &Logger{
		msgType:    t,
		streamName: n,
		batchSize:  b,
		buffer:     buf,
	}
	go l.sync()

	return l, nil
}

func (l *Logger) Log(msg interface{}) error {
	if !reflect.TypeOf(msg).AssignableTo(l.msgType) {
		// TODO(y): Add unit test for type compatibility checking.
		return fmt.Errorf("parameter (%+v) not assignable to %v", msg, l.msgType)
	}

	select {
	case l.buffer <- msg:
	case <-time.After(writeTimeout):
		// TODO(y): Add unit test for write timeout logic.
		return fmt.Errorf("dlog writes %+v timeout after %v", msg, writeTimeout)
	}

	return nil
}

func batchSize(t reflect.Type) (int, error) {
	b := int(maxBatchSize / t.Size())
	if b <= 0 {
		return 0, fmt.Errorf("Message size mustn't be bigger than %d", maxBatchSize)
	}
	return b, nil
}

func (l *Logger) sync() {
	ticker := time.NewTicker(syncPeriod)

	buf := make([]interface{}, 0, l.batchSize)

	for msg := range l.buffer {
		buf = append(buf, msg)

		f := false
		if len(buf) >= l.batchSize {
			f = true // Flush if buffer big enough.
		}

		select {
		case <-ticker.C:
			f = true // Flush periodically.
		default:
		}

		if f {
			l.flush(buf)
		}
	}
}

func (l *Logger) flush(buf []interface{}) {
	// TODO(y): Finish this.
	buf = buf[0:0]
}
