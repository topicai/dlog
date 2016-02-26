package dlog

import (
	"fmt"
	"log"
	"reflect"
	"strings"
	"time"

	"github.com/AdRoll/goamz/aws"
	"github.com/AdRoll/goamz/kinesis"
	"github.com/topicai/candy"
)

type Options struct {
	AccessKey string
	SecretKey string
	Region    string

	// 用于区分不同部署环境。dev环境、CI环境、staging环境、production环境.
	StreamNamePrefix string

	// 用于在测试时给创建的stream分配一个时间戳后缀, 确保每次执行unit test创建的stream的名字不同
	StreamNameSuffix string

	WriteTimeout time.Duration // 0 means wait forever.

	UseMockKinesis bool // By default this is false, which means using AWS Kinesis.
}

// streamName returns a string "prefix--typeName(msg)--suffix", or
// "prefix--typeName(msg)" if suffix is empty.  streamName panics for
// errors instead of returning them.
func (o *Options) streamName(msg interface{}) string {
	if len(o.StreamNamePrefix) <= 0 {
		log.Panicf("Options.Prefix mustn't be empty")
	}

	tname, e := fullMsgTypeName(reflect.TypeOf(msg))
	candy.Must(e)

	stream := fmt.Sprintf("%s--%s", o.StreamNamePrefix, tname)
	if len(o.StreamNameSuffix) > 0 {
		stream = fmt.Sprintf("%s--%s", stream, o.StreamNameSuffix)
	}

	if len(stream) > 128 {
		// http://docs.aws.amazon.com/kinesis/latest/APIReference/API_CreateStream.html#API_CreateStream_RequestParameters
		log.Panicf("stream name (%s) longer than 128 characters.", stream)
	}

	// We use the same name for Kinesis/Firehose stream and the
	// coupled S3 bucket, however, the name of Firehose-coupled S3
	// bucket cannot include capitalized characters.
	return strings.ToLower(stream)
}

func (o *Options) kinesis() KinesisInterface {
	if o.UseMockKinesis {
		return newKinesisMock()
	}

	return kinesis.New(
		aws.Auth{
			AccessKey: o.AccessKey,
			SecretKey: o.SecretKey},
		getAWSRegion(o.Region))
}
