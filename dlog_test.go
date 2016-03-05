package dlog

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

const (
	testingAccessKey  = "AKIAP4GWC7C4S5BYKMCA"
	testingSecretKey  = "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"
	testingRegion     = "cn-north-1"
	testingShardCount = 2
)

type click struct {
	Session string
	Element string
}

type impression struct {
	Session string
	Query   string
	Results []string // List of search results.
}

func TestLoggingToMockKinesis(t *testing.T) {
	assert := assert.New(t)

	l, e := NewLogger(&impression{}, &Options{
		WriteTimeout:   0, // Wait forever.
		SyncPeriod:     time.Second,
		UseMockKinesis: true,
		MockKinesis:    newKinesisMock(0),
	})
	assert.Nil(e)
	assert.NotNil(l)

	e = l.MockKinesis.CreateStream(l.streamName, 2)
	assert.Nil(e)

	storage := l.kinesis.(*kinesisMock).storage
	assert.NotNil(storage)

	// Logging wrong type writes nothing.
	assert.NotNil(l.Log(click{}))
	time.Sleep(2 * l.SyncPeriod) // Wait enough long for syncing.
	assert.Equal(0, len(storage))

	assert.Nil(l.Log(impression{Session: "0"}))
	assert.Equal(0, len(storage)) // No waiting for syncing.
	time.Sleep(2 * l.SyncPeriod)
	assert.Equal(1, len(storage)) // After waiting for syncing.
}

type WriteLogSuiteTester struct {
	suite.Suite

	options     *Options
	seachLogger *Logger
	clickLogger *Logger
	streamNames []string
}

// The SetupSuite method will be run by testify once, at the very
// start of the testing suite, before any tests are run.
func (s *WriteLogSuiteTester) SetupSuite() {

	s.options = &Options{
		AccessKey:        testingAccessKey,
		SecretKey:        testingSecretKey,
		Region:           testingRegion,
		StreamNamePrefix: "testing",
		StreamNameSuffix: fmt.Sprint(time.Now().UnixNano()),

		UseMockKinesis: true,
		MockKinesis:    newKinesisMock(0 * time.Second),
	}

	var err error
	s.seachLogger, err = NewLogger(&impression{}, s.options)
	s.Nil(err)

	s.clickLogger, err = NewLogger(&click{}, s.options)
	s.Nil(err)

	// create stream 1
	err = s.seachLogger.kinesis.CreateStream(s.seachLogger.streamName, testingShardCount)
	s.Nil(err)

	// create stream 2
	err = s.clickLogger.kinesis.CreateStream(s.clickLogger.streamName, testingShardCount)
	s.Nil(err)

	s.streamNames = []string{s.seachLogger.streamName, s.clickLogger.streamName}

	for { // waiting created stream's status to be active
		time.Sleep(1 * time.Second)
		resp1, err1 := s.seachLogger.kinesis.DescribeStream(s.seachLogger.streamName)
		s.Nil(err1)

		resp2, err2 := s.seachLogger.kinesis.DescribeStream(s.clickLogger.streamName)
		s.Nil(err2)

		status1 := strings.ToLower(string(resp1.StreamStatus))
		status2 := strings.ToLower(string(resp2.StreamStatus))
		if status1 == "active" && status2 == "active" {
			break
		}
	}
}

// The TearDownSuite method will be run by testify once, at the very
// end of the testing suite, after all tests have been run.
func (s *WriteLogSuiteTester) TearDownSuite() {
	if s.streamNames == nil || len(s.streamNames) == 0 {
		return
	}

	for _, streamName := range s.streamNames {
		err := s.seachLogger.kinesis.DeleteStream(streamName)
		s.Nil(err)
	}
}

func (s *WriteLogSuiteTester) TestWriteLog() {
	defer func() { // Recover if panicking to make sure TearDownSuite will be executed
		if r := recover(); r != nil {
			s.Fail(fmt.Sprint(r))
		}
	}()

	count := 20
	for i := 0; i < count; i++ {
		click := &click{
			Session: "Ethan",
			Element: "btnAddVenue" + strconv.Itoa(i),
		}

		err1 := s.clickLogger.Log(click)
		s.Nil(err1)

		search := &impression{
			Session: "Jack",
			Query:   "food" + strconv.Itoa(i),
			Results: []string{"apple", "banana"},
		}
		err2 := s.seachLogger.Log(search)
		s.Nil(err2)

		time.Sleep(200 * time.Millisecond)
	}

	time.Sleep(3 * time.Second) // make sure records in buf will be sent to Kinesis

	writtenClickRecords, _ := strconv.Atoi(s.clickLogger.writtenRecords.String())
	failedClickRecords, _ := strconv.Atoi(s.clickLogger.failedRecords.String())
	s.Equal(count, writtenClickRecords+failedClickRecords)

	writtenSearchRecords, _ := strconv.Atoi(s.seachLogger.writtenRecords.String())
	failedSearchRecords, _ := strconv.Atoi(s.seachLogger.failedRecords.String())
	s.Equal(count, writtenSearchRecords+failedSearchRecords)
}

func TestRunWriteLogSuite(t *testing.T) {
	suiteTester := new(WriteLogSuiteTester)
	suite.Run(t, suiteTester)
}


func TestLogWriteTimeout(t *testing.T) {
	assert := assert.New(t)

	l, e := NewLogger(&impression{}, &Options{
		WriteTimeout:   2 * time.Second,
		SyncPeriod:     10000 * time.Second, // set a long time to make time ticker will not trigger sync
		UseMockKinesis: true,
		MockKinesis:    newKinesisMock(600 * time.Second), // make latency big enough
	})
	assert.Nil(e)
	assert.NotNil(l)

	e = l.MockKinesis.CreateStream(l.streamName, 2)
	assert.Nil(e)

	for i := 0; i < 100; i++ { // write enough messages to make sure buf is full and trigger sync
		search := &impression{
			Session: "Jack",
			Query:   "food",
			Results: []string{strings.Repeat("1234567890", 1024*100)},
		}

		e = l.Log(search)
		if e != nil {
			break
		}
	}

	assert.NotNil(e)
	assert.True(strings.Contains(fmt.Sprint(e), "timeout after"))
}
