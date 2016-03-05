package dlog

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOptionsStreamName(t *testing.T) {
	assert := assert.New(t)

	type Impression struct {
		Query   string
		Results []string
	}

	opts := &Options{
		StreamNamePrefix: "production",
		StreamNameSuffix: ""}

	n, e := opts.streamName(Impression{})
	assert.Nil(e)
	assert.Equal("production--github.com-topicai-dlog.impression", n)

	opts = &Options{
		StreamNamePrefix: "dev",
		StreamNameSuffix: "12345"}

	n, e = opts.streamName(Impression{})
	assert.Nil(e)
	assert.Equal("dev--github.com-topicai-dlog.impression--12345", n)
}
