package dlog

import (
	"fmt"
	"log"
	"reflect"
	"regexp"
	"strings"

	"github.com/topicai/candy"
)

var (
	// We use full Go type name of log messages as the Kinesis
	// stream name. Because Kinesis requires that streams names
	// follow pattern [a-zA-Z0-9_.-]+, we require Go type name
	// compatible with this pattern.
	streamNameRegexp = regexp.MustCompile(`^[a-zA-Z0-9\.\-_]+$`)

	// All packages which contains Go struct types used for data
	// logging need to call dllog.RegisterType to add the type
	// into msgTypes, so that we can recreate a message variable
	// given the type name.  For more details, please refer to
	// README.md.
	msgTypes = make(map[string]reflect.Type)
)

func RegisterType(msg interface{}) {
	t, e := msgType(msg)
	candy.Must(e)

	n, e := fullMsgTypeName(msg)
	candy.Must(e)

	if tt, exists := msgTypes[n]; exists {
		if tt != t {
			log.Panicf("Type name %s already correspond to %v", n, tt)
		}
	} else {
		msgTypes[n] = t
	}
}

func msgType(msg interface{}) (reflect.Type, error) {
	t := reflect.TypeOf(msg)

	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("dlog message must be either *struct or struct")
	}

	return t, nil
}

func fullMsgTypeName(msg interface{}) (string, error) {
	t, e := msgType(msg)
	if e != nil {
		return "", e
	}

	if len(t.Name()) <= 0 {
		return "", fmt.Errorf("Cannot identity type name of dlog message")
	}

	if !streamNameRegexp.MatchString(t.Name()) {
		return "", fmt.Errorf("dlog message type name (%s) must match [a-zA-Z0-9_.-]+", t.Name())
	}

	if len(t.PkgPath()) <= 0 {
		return "", fmt.Errorf("Cannot identity package of dlog message type %v", t.PkgPath())
	}

	// Kinesis stream names and S3 bucket names cannot have '/'.
	name := strings.Join(
		[]string{
			strings.Replace(t.PkgPath(), "/", "-", -1),
			t.Name()},
		".")

	// Names of buckets coupled with Firehose streams cannot have capitalized characters.
	name = strings.ToLower(name)

	if !streamNameRegexp.MatchString(name) {
		return "", fmt.Errorf("dlog message full type name (%s) must match [a-zA-Z0-9_.-]+", name)
	}

	return name, nil
}
