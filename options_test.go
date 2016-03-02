package dlog

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOptionsStreamName(t *testing.T) {
	type Impression struct {
		Query   string
		Results []string
	}

	opts := &Options{
		StreamNamePrefix: "production",
		StreamNameSuffix: ""}

	assert.Equal(t,
		"production--github.com-topicai-dlog.impression",
		opts.streamName(Impression{}))

	opts = &Options{
		StreamNamePrefix: "dev",
		StreamNameSuffix: "12345"}

	assert.Equal(t,
		"dev--github.com-topicai-dlog.impression--12345",
		opts.streamName(Impression{}))
}
