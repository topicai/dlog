package dlog

import (
	"bytes"
	"crypto/md5"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"log"
	"reflect"
	"strings"
	"time"

	"github.com/AdRoll/goamz/aws"
	"github.com/AdRoll/goamz/kinesis"
)

const (
	// Message batches acceptable by Kinesis is no larger than 1MB.
	maxBatchSize = uintptr(1 * 1024 * 1024)

	// dlog writes into buffered channels. Here is the write timeout.
	writeTimeout = time.Second * 10

	// dlog syncs from buffered channels to Kinesis periodically.
	syncPeriod = time.Second
)

type Options struct {
	AccessKey string // Required
	SecretKey string // Required
	Region    string // Required

	// Required, stream name prefix, 一般用于区分不同部署环境。dev环境、CI环境、staging环境、production环境.
	StreamNamePrefix string

	// Optional, stream name suffix, 目前用于在测试时给创建的stream分配一个时间戳后缀, 确保每次执行unit test创建的stream的名字不同
	StreamNameSuffix string
}

type Logger struct {
	msgType    reflect.Type
	streamName string
	buffer     chan interface{}
	options    *Options
	kinesis    *kinesis.Kinesis
}

func NewLogger(example interface{}, options *Options) (*Logger, error) {
	t := reflect.TypeOf(example)

	n, e := fullMsgTypeName(t)
	if e != nil {
		return nil, e
	}

	sn, e := streamName(n, options.StreamNamePrefix, options.StreamNameSuffix)
	if e != nil {
		return nil, e
	}

	buf := make(chan interface{})

	k := kinesis.New(aws.Auth{
		AccessKey: options.AccessKey,
		SecretKey: options.SecretKey},
		getAWSRegion(options.Region))

	l := &Logger{
		msgType:    t,
		streamName: sn,
		buffer:     buf,
		options:    options,
		kinesis:    k,
	}
	go l.sync()

	return l, nil
}

func (l *Logger) Log(msg interface{}) error {
	if !reflect.TypeOf(msg).AssignableTo(l.msgType) {
		return fmt.Errorf("parameter (%+v) not assignable to %v", msg, l.msgType)
	}

	select {
	case l.buffer <- msg:
	case <-time.After(writeTimeout):
		// TODO(y): Add unit test for write timeout logic.
		return fmt.Errorf("dlog writes %+v timeout after %v", msg, writeTimeout)
	}

	return nil
}

func streamName(fullMsgTypeName, prefix, suffix string) (string, error) {
	if len(prefix) <= 0 {
		return "", fmt.Errorf("prefix must be non-empty string")
	}

	var result string

	if len(suffix) > 0 {
		result = strings.Join([]string{prefix, fullMsgTypeName, suffix}, "--")
	} else {
		result = strings.Join([]string{prefix, fullMsgTypeName}, "--")
	}

	if len(result) > 128 { // refer to: http://docs.aws.amazon.com/kinesis/latest/APIReference/API_CreateStream.html#API_CreateStream_RequestParameters
		return "", fmt.Errorf("stream name's length should not be longer than 128 characters")
	}

	return strings.ToLower(result), nil
}

func getAWSRegion(regionName string) aws.Region {
	if n := strings.ToLower(regionName); n == "cn-north-1" {
		return aws.Region{
			"cn-north-1",
			aws.ServiceInfo{"https://ec2.cn-north-1.amazonaws.com.cn", aws.V2Signature},
			"https://s3.cn-north-1.amazonaws.com.cn",
			"",
			true,
			true,
			"",
			"https://sns.cn-north-1.amazonaws.com.cn",
			"https://sqs.cn-north-1.amazonaws.com.cn",
			"",
			"https://iam.cn-north-1.amazonaws.com.cn",
			"https://elasticloadbalancing.cn-north-1.amazonaws.com.cn",
			"",
			"https://dynamodb.cn-north-1.amazonaws.com.cn",
			aws.ServiceInfo{"https://monitoring.cn-north-1.amazonaws.com.cn", aws.V4Signature},
			"https://autoscaling.cn-north-1.amazonaws.com.cn",
			aws.ServiceInfo{"https://rds.cn-north-1.amazonaws.com.cn", aws.V4Signature},
			"https://kinesis.cn-north-1.amazonaws.com.cn",
			"https://sts.cn-north-1.amazonaws.com.cn",
			"",
			"",
		}
	} else {
		return aws.Regions[n]
	}
}

func (l *Logger) sync() {
	ticker := time.NewTicker(syncPeriod)

	buf := make([][]byte, 0)

	for {
		f := false

		select {
		case msg := <-l.buffer:
			buf = append(buf, encode(msg))

			if len(buf) >= l.batchSize {
				log.Printf("buf size (%v) exceeds l.batchSize (%v)", len(buf), l.batchSize)
				f = true // Flush if buffer big enough.
			}
		case <-ticker.C:
			log.Print("sync time is up")
			f = true // Flush periodically.
		}

		if f {
			l.flush(buf)
		}
	}
}

func (l *Logger) flush(buf []interface{}) {
	defer func() { // Recover if panicking
		if r := recover(); r != nil {
			log.Printf("Recover from error (%v)", r)
		}
	}()

	if len(buf) <= 0 {
		return
	}

	entries := make([]kinesis.PutRecordsRequestEntry, 0, len(buf))

	for _, msg := range buf {
		data, e := getMsgData(msg)
		if e != nil {
			continue
		}
		entries = append(entries, kinesis.PutRecordsRequestEntry{
			Data:         data,
			PartitionKey: getPartitionKey(data),
		})
	}

	resp, err := l.kinesis.PutRecords(l.streamName, entries)
	if err != nil {
		log.Printf("error happens when call PutRecords (%v)", err)
		return
	}

	log.Printf("success call PutRecords (%v)", resp)
	buf = buf[0:0]
}

func getMsgData(msg interface{}) ([]byte, error) {
	var buf bytes.Buffer

	e := gob.NewEncoder(&buf).Encode(msg)
	if e != nil {
		return nil, e
	}

	return buf.Bytes(), e
}

func getPartitionKey(data []byte) string {
	m := md5.Sum(data)
	return hex.EncodeToString(m[:])
}
