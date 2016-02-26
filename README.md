# `dlog`

`dlog` is a Go package for distributed structure logging using Amazon
AWS Kinesis/Firehose.  It contains two perspectives:

1. called by Web servers to generate structured log messages, and
1. called by log message consumer programs to load and parse log
   messages.

## Motivation

### Log Streams

A Web app is usually composed of multiple micro-services. Take the
search engine as an example: the most important service is likely the
search service, which knows and can log the query of each session,
together with search results of that query.  The sequence of such
search log messages is usually called *search query stream*.

If the user clicked one or more search results in the Web browser, a
Javascript program will let the click service knows about the
click. Therefore the click server can log clicks of each session in
the *click log stream*.

It is often that the developers of the search engine would like to
join log messages from these two streams that share the same session
id into *session log stream*, because each session log message with a
click can be used as a positive labelled example for the online click
model training system.  Similarly, a session without any click shows
that the results sucks for the query, and should be used as a
negatively labelled example.

This example shows that it is important to collect log streams, to
join them and to use them for online training.  Usually, it is also
important to keep the session logs on persistant storage like HDFS or
AWS S3.  An example is online advertising system, in which, persistent
session log messages are the clue to charge advertisers by ad clicks.

Also it is noticable that each micro-service might have multiple
instances (processes) running.  And log messages from all of these
instances should go to the same log stream.

![Alt text](http://g.gravizo.com/g?
  digraph G {
  rankdir=LR;
  search_log [label="search log stream", shape=box];
  click_log [label="click log stream", shape=box];
  session_log [label="session log stream", shape=box];
  "search_service/0" -> search_log;
  "search_service/1" -> search_log;
  "search_service/3" -> search_log;
  "click_service/0" -> click_log;
  "click_service/1" -> click_log;
  click_log -> "session log joiner";
  search_log -> "session log joiner" -> session_log;
  search_log -> "S3/search log";
  click_log -> "S3/click log";
  session_log -> "S3/session log";
  }
)


### AWS Kinesis/Firehose Streams

AWS Kinesis is a log collecting service, where users can create
*streams*, where each stream is like a distributed queue -- client
programs (often called *producers*) can write log messages into
Kinesis streams, and *consumer* programs can read messages out from
streams and save them somewhere.

AWS Firehose provides a special kind of Kinesis stream, called
Firehose streams.  Each Firehose stream is coupled with a consumer
program which constantly read log messages from the stream and save
them onto user-specified S3 buckets.

In the design of `dlog`, we use Kinesis/Firehose streams as log stream
in above figure.


## Design

### Naming of Streams/S3 Buckets/Go Types

Both the producer and consumer programs might use `dlog`. The
procedure writes some Go struct-typed variables into streams, and the
consumer would like to know the Go type so they can parse logs read
from streams.

To do this, we name Kinesis/Firehose streams by the Go struct type
(plus some more information, like a prefix to scope the usage, like
`dev`, `staging` and `production`, as well as a suffix to make the
name unique, for example, required by unit testing.  We also name the
S3 bucket coupled with Firehose streams the same way.

Let us consider the aforementioned example of search engine.  Suppose
that the producer of the search log stream is in Go package
`github.com/topicai/search`, and the log struct type
`SearchImpression` is defined as:

    type SearchImpression struct {
        Session string
        Query   string
	    Results []string // List of search results.
    }


We create a Kinesis/Firehose stream
`staging--github.com-topicai-search.SearchImpression` for integration
test, and a `production--github.com-topicai-search.SearchImpression`
for production use.  Note that according to
[Kinesis document](http://docs.aws.amazon.com/kinesis/latest/APIReference/API_CreateStream.html#API_CreateStream_RequestSyntax), the name of streams must be

```
[a-zA-Z0-9_.-]+
```

The same constraint applys to S3 buckets, but ***S3 buckets used by
Firehose streams cannot have uppercase letter***.  So we name the S3
bucket `staging--github.com-topicai-search.searchimpression` and
`production--github.com-topicai-search.searchimpression`


The streams for unit testing are named
`dev--github.com-topicai-search.SearchImpression--123456`, where
`123456` is a placeholder to make the stream unique.  Similarly, the
S3 bucket used in unit testing is
`dev--github.com-topicai-search.searchimpression--123456`.


### Rules of Naming

As a summarization of above example, we have the folloing rules for
naming streams given a Go type, like `SearchImpression`:

1. Given an instance of the Go struct type, say
   `msg:=SearchImpression{}`, we can get the type
   
   ```
   t:=reflect.TypeOf(msg)
   ```

1. Given the type, we can have its package name and
   type name

   ```
   pkg:=t.PkgPath()
   tn:=t.Name()
   ```

1. The full name is

   ```
   full:=strings.Replace(pkg, "/", "-") + "." + tn
   ```

   The replacement is necessary because "/" is not allowd in stream
   and bucket names.

1. The prefix (`dev`) and suffix (`123456`) are added with delimitor
   `--` to form the Kinesis/Firehose steram name: 

   ```
   sname := strings.Join([]string{prefix, full, suffix}, "--")
   ```

1. The Firehose bucket name must be all lower-cased:

    ```
	bname := strings.ToLower()
	```


Then, given a bucket name `bname`, we can extract the Go type name by:


```
full := strings.Split(bname, "--")[1]
```

then we can have the package path and type name:

```
pkg := strings.Split(full, ".")[0]
tn  := strings.Split(full, ".")[1]
```

It is notable that tn is all lower-cased.  We will describe how to
create an variable (instance) from `full` in the next section.



### Register Types for Parsing

The naming of streams and buckets are important -- the consumer side
of `dlog` relies on the bucket name to know the format of log
messages.

Go language does support creating variables (instance) given a type
represented by `reflect.Type`, but it doesn't support creations of
variables from type names, like `full` in above example.

A simple solution is to require that the package
`github.com/topicai/search` to register type `SearchImpression` into a
global mapping:

```
var nameToType map[string]reflect.Type
```
defined in package `dlog`.

This is resonable and techniclaly viable if in the source code file
where `SearchImpression` is defiend, we write:

```
func init() {
    dlog.RegisterType(SearchImpression{})
}
```

where `dlog.RegisterType` adds key-value pair `strings.ToLower(full)`
and `reflect.TypeOf(SearhImpression{})` into `nameToType`.

Given `nameToType` and `dlog.RegisterType`, once we have bucket name
`bname`, we can create an log message instance

```
full := strings.Split(bname, "--")[1]
if t, ok := nameToType[full]; ok {
    v := reflect.New(t)
	gob.NewDecoder(s3BucketFile).DecodeValue(v)
} else {
	return fmt.Errorf("Unknow type name %s", full)
}
```

For a more complete example, please refer to http://play.golang.org/p/V4NYaFSSY-



### Buffered Write to Kinesis

We can use either Kinesis API `PutRecord` to send a single log message
to Kinesis server, or use `PutRecords` to put a slice of messages as a
batch.  Usually, we should use the latter, because each log message is
much smaller than the 1MB limit of batch size.  Do we this batching,
we need a buffer.  And considering that multiple threads are likely
write through the same buffer, we want a thread-safe implementation of
buffer.  And Go happens have one -- the Go channel.

![Alt text](http://g.gravizo.com/g?
digraph G {
rankdir=LR;
buffer [label="chan interface", shape=box];
kinesis [label="Kinesis/Firehose stream", shape=box];
s3 [label="S3 bucket", shape=box];
"write goroutine 0" -> buffer;
"write goroutine 1" -> buffer;
"write goroutine 2" -> buffer;
buffer -> "sync goroutine" -> kinesis -> "Firehose persistency" -> s3;
}
)

Given that Kinesis prefers the maximam batch size to be 1MB, and
`reflect.Type.Size()` returns the size of a Go variable in bytes, we
can easily compute the batch size as 1MB/msgSize.


If the Kinesis/Firehose service runs slower than the sync goroutine,
according to AWS document, we can increase the number of Kinesis
shards.

If sync goroutine runs slower than write goroutines, the Go channel
might be full and writes are blocked.  Since clients might not want to
be blocked for too long time, we should introduce a write timeout
here using Go's `select` and `time.After()`.

