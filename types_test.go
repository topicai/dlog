package dlog

import (
	"fmt"
	"strings"
	"testing"

	"reflect"

	"github.com/stretchr/testify/assert"
)

func TestFullMsgTypeName(t *testing.T) {
	assert := assert.New(t)

	type LocalType struct {
		Name string
	}

	s, e := fullMsgTypeName(reflect.TypeOf(LocalType{}))
	assert.Nil(e)
	assert.Equal("github.com-topicai-dlog.localtype", s)

	s, e = fullMsgTypeName(reflect.TypeOf(&LocalType{}))
	assert.Nil(e)
	assert.Equal("github.com-topicai-dlog.localtype", s)

	s, e = fullMsgTypeName(reflect.TypeOf(struct{ Name string }{Name: "a name"}))
	assert.NotNil(e)
	assert.True(strings.Contains(fmt.Sprint(e), "Cannot identity type name of dlog message"))

}
