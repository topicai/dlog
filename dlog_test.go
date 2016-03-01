package dlog

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	// change following 2 arguments before test
	testingAccessKey  = "AKIAP5BYKMCA"
	testingSecretKey  = "3uqJbLQGRFtyMCb0zT"

	testingRegion     = "cn-north-1"
	testingShardCount = 2
)

var (
	testingStreamNamePrefix = "t"
	testingStreamNameSuffix = strconv.FormatInt(time.Now().UnixNano(), 10)
)

type ClickImpression struct {
	Session string
	Element string
}

type SearchImpression struct {
	Session string
	Query   string
	Results []string // List of search results.
}

func TestNewLogger(t *testing.T) {
	assert := assert.New(t)

	l, e := NewLogger(&SearchImpression{}, &Options{
		AccessKey:        testingAccessKey,
		SecretKey:        testingSecretKey,
		Region:           testingRegion,
		StreamNamePrefix: testingStreamNamePrefix,
		StreamNameSuffix: testingStreamNameSuffix,
	})
	assert.NotNil(l)
	assert.Nil(e)
}

func TestLog(t *testing.T) {
	assert := assert.New(t)

	l, e := NewLogger(&SearchImpression{}, &Options{
		AccessKey:        testingAccessKey,
		SecretKey:        testingSecretKey,
		Region:           testingRegion,
		StreamNamePrefix: testingStreamNamePrefix,
		StreamNameSuffix: testingStreamNameSuffix,
	})
	assert.NotNil(l)
	assert.Nil(e)

	incorrectMsg := &ClickImpression{
		Session: "session1",
		Element: "btnAddVenue",
	}
	e = l.Log(incorrectMsg)
	assert.NotNil(e)
	assert.True(strings.Contains(fmt.Sprint(e), "not assignable to"))
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
		StreamNamePrefix: testingStreamNamePrefix,
		StreamNameSuffix: testingStreamNameSuffix,
	}

	var err error
	s.seachLogger, err = NewLogger(&SearchImpression{}, s.options)
	s.Nil(err)

	s.clickLogger, err = NewLogger(&ClickImpression{}, s.options)
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
		s.T().Logf("Delete stream %v", streamName)
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

	for i := 0; i < 20; i++ {
		click := &ClickImpression{
			Session: "Ethan",
			Element: "btnAddVenue" + strconv.Itoa(i),
		}

		err1 := s.clickLogger.Log(click)
		s.Nil(err1)

		search := &SearchImpression{
			Session: "Jack",
			Query:   "food" + strconv.Itoa(i),
			Results: []string{"apple", "banana"},
		}
		err2 := s.seachLogger.Log(search)
		s.Nil(err2)

		time.Sleep(200 * time.Millisecond)
	}
}

func TestRunWriteLogSuite(t *testing.T) {
	suiteTester := new(WriteLogSuiteTester)
	suite.Run(t, suiteTester)
}
