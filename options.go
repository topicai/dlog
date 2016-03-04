package dlog

import (
	"fmt"
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

	// StreamNamePrefix is one of "testing", "staging", or
	// "production".  When StreamNamePrefix is "testing",
	// StreamNameSuffix is often a unique timestamp, so that the
	// stream name is unique for every run of unit test.
	StreamNamePrefix string
	StreamNameSuffix string

	// The timeout for Logger.Log to write into a channel, when
	// the channel is blocked at reading end.  0 means wait
	// forever.
	WriteTimeout time.Duration

	// a sync goroutine reads messages from the channels, and send
	// packed messages to Kinesis periodically. 0 means 1 second.
	SyncPeriod time.Duration

	// Retry if failed to sync to Kinesis. 0 means not retry.
	MaxRetryTimes int

	UseMockKinesis bool // By default this is false, which means using AWS Kinesis.
	MockKinesis    KinesisInterface
}

// streamName returns a string "prefix--typeName(msg)--suffix", or
// "prefix--typeName(msg)" if suffix is empty.
func (o *Options) streamName(msg interface{}) (string, error) {
	if !o.UseMockKinesis && len(o.StreamNamePrefix) <= 0 {
		return "", fmt.Errorf("Options.Prefix mustn't be empty")
	}

	tname, e := fullMsgTypeName(msg)
	candy.Must(e)

	stream := fmt.Sprintf("%s--%s", o.StreamNamePrefix, tname)
	if len(o.StreamNameSuffix) > 0 {
		stream = fmt.Sprintf("%s--%s", stream, o.StreamNameSuffix)
	}

	if len(stream) > 128 {
		// http://docs.aws.amazon.com/kinesis/latest/APIReference/API_CreateStream.html#API_CreateStream_RequestParameters
		return "", fmt.Errorf("stream name (%s) longer than 128 characters.", stream)
	}

	// We use the same name for Kinesis/Firehose stream and the
	// coupled S3 bucket, however, the name of Firehose-coupled S3
	// bucket cannot include capitalized characters.
	return strings.ToLower(stream), nil
}

func (o *Options) kinesis() (KinesisInterface, error) {
	if o.UseMockKinesis {
		if o.MockKinesis == nil {
			return nil, fmt.Errorf("MockKinesis mustn't be nil if UseMockKinesis is true")
		}

		return o.MockKinesis, nil
	}

	return kinesis.New(
		aws.Auth{
			AccessKey: o.AccessKey,
			SecretKey: o.SecretKey},
		awsRegion(o.Region)), nil
}

func awsRegion(regionName string) aws.Region {
	if n := strings.ToLower(regionName); n == "cn-north-1" {
		// NOTE: github.com/AdRoll/goamz/aws.Regions doesn't include endpoints of Kinesis.
		return aws.Region{
			Name: "cn-north-1",
			EC2Endpoint: aws.ServiceInfo{
				Endpoint: "https://ec2.cn-north-1.amazonaws.com.cn",
				Signer:   aws.V2Signature},
			S3Endpoint:           "https://s3.cn-north-1.amazonaws.com.cn",
			S3BucketEndpoint:     "",
			S3LocationConstraint: true,
			S3LowercaseBucket:    true,
			SDBEndpoint:          "",
			SNSEndpoint:          "https://sns.cn-north-1.amazonaws.com.cn",
			SQSEndpoint:          "https://sqs.cn-north-1.amazonaws.com.cn",
			SESEndpoint:          "",
			IAMEndpoint:          "https://iam.cn-north-1.amazonaws.com.cn",
			ELBEndpoint:          "https://elasticloadbalancing.cn-north-1.amazonaws.com.cn",
			KMSEndpoint:          "",
			DynamoDBEndpoint:     "https://dynamodb.cn-north-1.amazonaws.com.cn",
			CloudWatchServicepoint: aws.ServiceInfo{
				Endpoint: "https://monitoring.cn-north-1.amazonaws.com.cn",
				Signer:   aws.V4Signature},
			AutoScalingEndpoint: "https://autoscaling.cn-north-1.amazonaws.com.cn",
			RDSEndpoint: aws.ServiceInfo{
				Endpoint: "https://rds.cn-north-1.amazonaws.com.cn",
				Signer:   aws.V4Signature},
			KinesisEndpoint:        "https://kinesis.cn-north-1.amazonaws.com.cn",
			STSEndpoint:            "https://sts.cn-north-1.amazonaws.com.cn",
			CloudFormationEndpoint: "",
			ElastiCacheEndpoint:    "",
		}
	} else {
		return aws.Regions[n]
	}
}
