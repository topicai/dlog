package dlog

import (
	"testing"

	"github.com/AdRoll/goamz/kinesis"
	"github.com/stretchr/testify/assert"
)

func TestKinesisPutRecords(t *testing.T) {
	assert := assert.New(t)

	m := newKinesisMock()

	assert.Panics(func() {
		m.PutRecords("", make([]kinesis.PutRecordsRequestEntry, 1))
	})

	assert.Panics(func() {
		m.PutRecords("=", make([]kinesis.PutRecordsRequestEntry, 1))
	})

	assert.Panics(func() {
		m.PutRecords("/", make([]kinesis.PutRecordsRequestEntry, 1))
	})

	assert.NotPanics(func() {
		m.PutRecords("Stream.0", make([]kinesis.PutRecordsRequestEntry, 1))
	})

	assert.NotPanics(func() {
		m.PutRecords("dev--github.com-topicai-dlog.Log--0123", make([]kinesis.PutRecordsRequestEntry, 1))
	})

}
