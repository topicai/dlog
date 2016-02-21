package dlog

import (
	"encoding/gob"
	"fmt"
	"log"
	"os"
	"path"
	"reflect"
	"sync"
	"time"

	. "github.com/topicai/candy"
)

type Logger struct {
	mutex   sync.Mutex // Serialize log messages and protect log shard file rotation.
	dir     string
	msgType reflect.Type
	sizeCap int64
	file    *os.File
	encoder *gob.Encoder
}

func NewLogger(dir string, msgExample interface{}, sizeCap int64) (*Logger, error) {
	if e := mkDirIfNotExist(dir); e != nil {
		return nil, e
	}

	t := reflect.TypeOf(msgExample)
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("msgExample parameter must be a struct, but not %v", t.Kind())
	}
	if len(t.Name()) <= 0 {
		return nil, fmt.Errorf("msgExample must be a named struct type")
	}

	return &Logger{
		dir:     dir,
		msgType: t,
		sizeCap: sizeCap}, nil
}

func (l *Logger) Log(msg interface{}) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if t := reflect.TypeOf(msg); !t.AssignableTo(l.msgType) {
		log.Panicf("type of msg (%v) is not assignable to %v", t, l.msgType)
	}

	if l.file == nil {
		Must(l.rotateFile())
	}

	if fi, _ := l.file.Stat(); fi.Size() > l.sizeCap {
		Must(l.rotateFile())
	}

	if e := l.encoder.Encode(msg); e != nil {
		log.Printf("Logger.Log encoding error %v. Rotate file because the current file might contain a broken record.", e)
		Must(l.rotateFile())
	}
}

func mkDirIfNotExist(dir string) error {
	if _, e := os.Stat(dir); e != nil && os.IsNotExist(e) {
		if e := os.MkdirAll(dir, 0744); e != nil {
			return fmt.Errorf("Failed to create dir (%v) : %v", dir, e)
		}
	}

	return nil
}

func (l *Logger) rotateFile() error {
	if l.file != nil {
		l.file.Close()
	}

	dir := path.Join(l.dir, l.msgType.Name())
	if e := mkDirIfNotExist(dir); e != nil {
		return e
	}

	file := path.Join(dir, fmt.Sprintf("%016x-%016x", time.Now().UnixNano(), os.Getpid()))
	if f, e := os.Create(file); e != nil {
		return fmt.Errorf("Failed create log rotation file %v: %v", file, e)
	} else {
		l.file = f
		l.encoder = gob.NewEncoder(f)
	}

	return nil
}
