package dlog

import (
	"log"
	"time"

	"github.com/AdRoll/goamz/kinesis"
)

type KinesisInterface interface {
	PutRecords(streamName string, records []kinesis.PutRecordsRequestEntry) (resp *kinesis.PutRecordsResponse, err error)
	CreateStream(name string, shardCount int) error
	DescribeStream(name string) (resp *kinesis.StreamDescription, err error)
	DeleteStream(name string) error
}

type kinesisMock struct {
	// Mapping from steam name to batches of batches
	storage map[string][][]kinesis.PutRecordsRequestEntry

	// simulate lantency that sync to Kinesis
	putRecordLatency time.Duration
}

func newKinesisMock(putRecordsLatency time.Duration) *kinesisMock {
	return &kinesisMock{
		storage:          make(map[string][][]kinesis.PutRecordsRequestEntry),
		putRecordLatency: putRecordsLatency,
	}
}

func (mock *kinesisMock) PutRecords(streamName string, records []kinesis.PutRecordsRequestEntry) (resp *kinesis.PutRecordsResponse, err error) {
	if !streamNameRegexp.MatchString(streamName) {
		log.Panicf("Invalid stream name %s", streamName)
	}

	if len(records) == 0 {
		log.Panicf("records length == 0")
	}

	time.Sleep(mock.putRecordLatency)

	mock.storage[streamName] = append(mock.storage[streamName], records)
	return &kinesis.PutRecordsResponse{
		FailedRecordCount: 0, // Always success.
		Records:           nil}, nil
}

func (mock *kinesisMock) CreateStream(name string, shardCount int) error {
	log.Panic("kinesisMock.CreateStream is not implemented")
	return nil
}

func (mock *kinesisMock) DescribeStream(name string) (resp *kinesis.StreamDescription, err error) {
	log.Panic("kinesisMock.DescribeStream is not implemented")
	return nil, nil
}

func (mock *kinesisMock) DeleteStream(name string) error {
	log.Panic("kinesisMock.DeleteStream is not implemented")
	return nil
}
