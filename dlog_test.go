package dlog

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const (
	// AWS IAM role of AWS account arn:aws-cn:iam::260363350995:user/CITester
	testingAccessKey  = "AKIAP4GWC7C4S5BYKMCA"
	testingSecretKey  = "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"
	testingRegion     = "cn-north-1"
	testingShardCount = 2
)

var (
	testingStreamNamePrefix = "dev"
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
