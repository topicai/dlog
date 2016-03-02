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

	// Registering pointer to struct is like registering struct.
	s, e = fullMsgTypeName(reflect.TypeOf(&LocalType{}))
	assert.Nil(e)
	assert.Equal("github.com-topicai-dlog.localtype", s)

	// Registering unnamed struct.
	s, e = fullMsgTypeName(reflect.TypeOf(struct{ Name string }{Name: "a name"}))
	assert.NotNil(e)
	assert.True(strings.Contains(fmt.Sprint(e), "Cannot identity type name of dlog message"))
}

func TestRegisterType(t *testing.T) {
	assert := assert.New(t)

	type SomeType struct{}
	type AnotherType struct{}
	type anotherType struct{}

	RegisterType(SomeType{})
	assert.Equal(reflect.TypeOf(SomeType{}),
		msgTypes["github.com-topicai-dlog.sometype"])

	assert.Panics(func() { RegisterType(&SomeType{}) }) // Already registered the plain struct type.

	RegisterType(&AnotherType{}) // Registering pointer to struct is like registering struct.
	assert.Equal(reflect.TypeOf(&AnotherType{}),
		msgTypes["github.com-topicai-dlog.anothertype"])

	assert.Panics(func() { RegisterType(anotherType{}) }) // msgTypes keys are lower-case strings.
}
