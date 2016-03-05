package dlog

import (
	"strings"
	"testing"
	"time"

	"github.com/AdRoll/goamz/kinesis"
	"github.com/stretchr/testify/assert"
)

func TestKinesisMock(t *testing.T) {
	assert := assert.New(t)

	m := newKinesisMock(0)

	assert.Error(
		m.CreateStream("", 2),
	)

	assert.Error(
		m.CreateStream("=", 2),
	)

	assert.Error(
		m.CreateStream("/", 2),
	)

	assert.NoError(
		m.CreateStream("Stream.0", 2),
	)

	func(){
		m.CreateStream("dev--github.com-topicai-dlog.Log--1111", 2)
		_, e := m.PutRecords("dev--github.com-topicai-dlog.Log--9999", make([]kinesis.PutRecordsRequestEntry, 1))
		assert.Error(e)
	}()

	func(){
		m.CreateStream("dev--github.com-topicai-dlog.Log--0123", 2)
		_, e := m.PutRecords("dev--github.com-topicai-dlog.Log--0123", make([]kinesis.PutRecordsRequestEntry, 1))
		assert.NoError(e)
	}()

	func(){
		streamName := "dev--github.com-topicai-dlog.Log--0123"

		m.CreateStream(streamName, 2)
		desc, e := m.DescribeStream(streamName)
		assert.NoError(e)
		assert.Equal("active", strings.ToLower(string(desc.StreamStatus)))

		_, e = m.PutRecords(streamName, make([]kinesis.PutRecordsRequestEntry, 1))
		assert.NoError(e)

		e = m.DeleteStream(streamName)
		assert.NoError(e)
	}()
}

func TestLatencyOfKinesisMockPutRecords(t *testing.T) {
	assert := assert.New(t)

	streamName := "dev--github.com-topicai-dlog.Log--0123"

	m := newKinesisMock(3 * time.Second)

	m.CreateStream(streamName, 2)
	_, e := m.PutRecords(streamName, make([]kinesis.PutRecordsRequestEntry, 1))
	assert.NoError(e)
}
