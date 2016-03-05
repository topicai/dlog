package dlog

import (
	"time"

	"fmt"
	"github.com/AdRoll/goamz/kinesis"
	"errors"
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

	// created streams' names
	streamNames []string
}

func newKinesisMock(putRecordsLatency time.Duration) *kinesisMock {
	return &kinesisMock{
		storage:          make(map[string][][]kinesis.PutRecordsRequestEntry),
		putRecordLatency: putRecordsLatency,
		streamNames:      make([]string, 0),
	}
}

func (mock *kinesisMock) PutRecords(streamName string, records []kinesis.PutRecordsRequestEntry) (resp *kinesis.PutRecordsResponse, err error) {

	if !streamNameRegexp.MatchString(streamName) {
		return nil, fmt.Errorf("Invalid stream name %s", streamName)
	}

	if !mock.find(streamName) {
		return nil, fmt.Errorf("Not found stream %s", streamName)
	}

	if len(records) == 0 {
		return nil, errors.New("records length == 0")
	}

	time.Sleep(mock.putRecordLatency)

	mock.storage[streamName] = append(mock.storage[streamName], records)
	return &kinesis.PutRecordsResponse{
		FailedRecordCount: 0, // Always success.
		Records:           nil}, nil
}

func (mock *kinesisMock) CreateStream(name string, shardCount int) error {

	if !streamNameRegexp.MatchString(name) {
		return fmt.Errorf("Invalid stream name %s", name)
	}

	if mock.find(name) {
		return fmt.Errorf("Stream already exists %s", name)
	}

	mock.streamNames = append(mock.streamNames, name)
	return nil
}

func (mock *kinesisMock) DescribeStream(name string) (resp *kinesis.StreamDescription, err error) {

	if !streamNameRegexp.MatchString(name) {
		return nil, fmt.Errorf("Invalid stream name %s", name)
	}

	if !mock.find(name) {
		return nil, fmt.Errorf("Not found stream %s", name)
	}

	resp = &kinesis.StreamDescription{
		StreamName:   name,
		StreamStatus: "Active",
	}
	return resp, nil
}

func (mock *kinesisMock) DeleteStream(name string) error {

	if !streamNameRegexp.MatchString(name) {
		return fmt.Errorf("Invalid stream name %s", name)
	}

	if !mock.find(name) {
		return fmt.Errorf("Not found stream %s", name)
	}

	newStreamNames := make([]string, 0, len(mock.streamNames) - 1)

	for _, v := range mock.streamNames {
		if v != name {
			newStreamNames = append(newStreamNames, v)
		}
	}

	mock.streamNames = newStreamNames
	return nil
}

func (mock *kinesisMock) find(streamName string) bool {
	if len(mock.streamNames) <= 0 {
		return false
	}

	for _, v := range mock.streamNames {
		if v == streamName {
			return true
		}
	}

	return false
}

type brokenKinesisMock struct {
	*kinesisMock
}

func newBrokenKinesisMock() *brokenKinesisMock {
	return &brokenKinesisMock{
		kinesisMock: newKinesisMock(0),
	}
}

func (mock *brokenKinesisMock) PutRecords(streamName string, records []kinesis.PutRecordsRequestEntry) (resp *kinesis.PutRecordsResponse, err error) {
	return nil, fmt.Errorf("Kinesis is broken")
}
