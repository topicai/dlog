package dlog

import (
	"fmt"
	"reflect"
	"strings"
)

func fullMsgTypeName(t reflect.Type) (string, error) {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		return "", fmt.Errorf("dlog message must be either *struct or struct")
	}

	if len(t.Name()) <= 0 {
		return "", fmt.Errorf("Cannot identity type name of dlog message")
	}

	if !pattern.MatchString(t.Name()) {
		return "", fmt.Errorf("dlog message type name (%s) must match [a-zA-Z0-9_.-]+", t.Name())
	}

	if len(t.PkgPath()) <= 0 {
		return "", fmt.Errorf("Cannot identity package of dlog message type %v", t.PkgPath())
	}

	stream := fmt.Sprintf("%s.%s",
		strings.Replace(t.PkgPath(), "/", "-", -1),
		t.Name())

	if !pattern.MatchString(stream) {
		return "", fmt.Errorf("dlog message full type name (%s) must match [a-zA-Z0-9_.-]+", stream)
	}

	return stream, nil
}
