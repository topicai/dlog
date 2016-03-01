package dlog

import (
	. "github.com/AdRoll/goamz/aws"
)

// Replace github.com/AdRoll/goamz/aws/regions.CNNorth1, which lack the URL of Kinesis
var UpdatedCNNorth1 = Region{
	"cn-north-1",
	ServiceInfo{"https://ec2.cn-north-1.amazonaws.com.cn", V2Signature},
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
	ServiceInfo{"https://monitoring.cn-north-1.amazonaws.com.cn", V4Signature},
	"https://autoscaling.cn-north-1.amazonaws.com.cn",
	ServiceInfo{"https://rds.cn-north-1.amazonaws.com.cn", V4Signature},
	"https://kinesis.cn-north-1.amazonaws.com.cn",
	"https://sts.cn-north-1.amazonaws.com.cn",
	"",
	"",
}
