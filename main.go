package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"io/ioutil"
	"mime"
	"os"
	// "strconv"
	"flag"
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/s3"
	configfile "github.com/crowdmob/goconfig"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

var configFilename string
var keepBufferFiles bool
var debug bool
var shouldOutputVersion bool
var hostsStr string
var hosts []string
var config *configfile.ConfigFile

const (
	VERSION                            = "0.1"
	ONE_MINUTE_IN_NANOS                = 60000000000
	S3_REWIND_IN_DAYS_BEFORE_LONG_LOOP = 14
	DAY_IN_SECONDS                     = 24 * 60 * 60
)

type ChunkBuffer struct {
	File           *os.File
	FilePath       *string
	MaxAgeInMins   int64
	MaxSizeInBytes int64
	Topic          *string
	Partition      int64
	Offset         uint64
	InitialOffset  uint64
	expiresAt      int64
	length         int64
}

type BackupTask struct {
	Topic     string
	Partition int64
	Offset    uint64
}

func (chunkBuffer *ChunkBuffer) BaseFilename() string {
	return fmt.Sprintf("kafka-s3-go-consumer-buffer-topic_%s-partition_%d-offset_%d-", *chunkBuffer.Topic, chunkBuffer.Partition, chunkBuffer.Offset)
}

func (chunkBuffer *ChunkBuffer) CreateBufferFileOrPanic() {
	tmpfile, err := ioutil.TempFile(*chunkBuffer.FilePath, chunkBuffer.BaseFilename())
	chunkBuffer.File = tmpfile
	chunkBuffer.expiresAt = time.Now().UnixNano() + (chunkBuffer.MaxAgeInMins * ONE_MINUTE_IN_NANOS)
	chunkBuffer.length = 0
	if err != nil {
		fmt.Errorf("Error opening buffer file: %#v\n", err)
		panic(err)
	}
}

func (chunkBuffer *ChunkBuffer) TooBig() bool {
	fmt.Println("check size", chunkBuffer.length, ">=", chunkBuffer.MaxSizeInBytes)
	return chunkBuffer.length >= chunkBuffer.MaxSizeInBytes
}

func (chunkBuffer *ChunkBuffer) TooOld() bool {
	return time.Now().UnixNano() >= chunkBuffer.expiresAt
}

func (chunkBuffer *ChunkBuffer) NeedsRotation() bool {
	return chunkBuffer.TooBig() || chunkBuffer.TooOld()
}

func FetchLastCommittedOffset(bucket *s3.Bucket, topic *string, partition int64) (uint64, error) {
	prefix := S3TopicPartitionPrefix(topic, partition)
	keyMarker := ""

	// First, do a few checks for shortcuts for checking backwards: focus in on the 14 days.
	// Otherwise just loop forward until there aren't any more results
	currentDay := time.Now()
	for i := 0; i < S3_REWIND_IN_DAYS_BEFORE_LONG_LOOP; i++ {
		testPrefix := fmt.Sprintf("%s%s", prefix, S3DatePrefix(&currentDay))
		println("prefix", testPrefix)
		results, err := bucket.List(prefix, "", keyMarker, 0)
		if err != nil {
			println(err.Error())
		}
		if err == nil && len(results.Contents) > 0 {
			prefix = testPrefix
			break
		}
		currentDay = currentDay.Add(-1 * time.Duration(DAY_IN_SECONDS) * time.Second)
	}

	lastKey := ""
	offset := uint64(0)
	moreResults := true
	for moreResults {
		results, err := bucket.List(prefix, "", keyMarker, 0)
		if err != nil {
			return 0, err
		}

		if len(results.Contents) == 0 { // empty request, done
			break
		}

		lastKey = results.Contents[len(results.Contents)-1].Key
		keyMarker = lastKey

		fmt.Println("lastKey", lastKey)
		_, offset = ParseS3KeyForOffsets(lastKey)
		fmt.Println("offset", offset)
		moreResults = results.IsTruncated
	}
	return offset, nil
}

func ParseS3KeyForOffsets(key string) (startOffset uint64, endOffset uint64) {
	baseName := path.Base(key)
	tokens := strings.Split(baseName, "_")

	if len(tokens) != 5 {
		return
	}

	fmt.Println(tokens[3], tokens[4])
	startOffset, _ = strconv.ParseUint(tokens[3], 10, 64)
	endOffset, _ = strconv.ParseUint(tokens[4], 10, 64)
	return
}

func S3DatePrefix(t *time.Time) string {
	return fmt.Sprintf("%d/%d/%d/", t.Year(), t.Month(), t.Day())
}

func S3TopicPartitionPrefix(topic *string, partition int64) string {
	return fmt.Sprintf("%s/p%d/", *topic, partition)
}

func KafkaMsgGuidPrefix(topic *string, partition int64) string {
	return fmt.Sprintf("t_%s-p_%d-o_", *topic, partition)
}

func S3KafkaChunkKey(topic *string, partition int64, startOffset uint64, endOffset uint64) string {
	return fmt.Sprintf("clusterid_%s_%d_%d_%d", *topic, partition, startOffset, endOffset)
}

func (chunkBuffer *ChunkBuffer) PutMessage(msg *sarama.ConsumerEvent) {
	uuid := []byte(fmt.Sprintf("%s%d|", KafkaMsgGuidPrefix(chunkBuffer.Topic, chunkBuffer.Partition), msg.Offset))
	lf := []byte("\n")
	chunkBuffer.Offset = uint64(msg.Offset)
	chunkBuffer.File.Write(uuid)
	chunkBuffer.File.Write(msg.Value)
	chunkBuffer.File.Write(lf)

	chunkBuffer.length += int64(len(uuid)) + int64(len(msg.Value)) + int64(len(lf))
}

func (chunkBuffer *ChunkBuffer) StoreToS3AndRelease(s3bucket *s3.Bucket) (bool, error) {
	var s3path string
	var err error

	if debug {
		fmt.Printf("Closing bufferfile: %s\n", chunkBuffer.File.Name())
	}
	chunkBuffer.File.Close()

	contents, err := ioutil.ReadFile(chunkBuffer.File.Name())
	if err != nil {
		return false, err
	}

	if len(contents) <= 0 {
		if debug {
			fmt.Printf("Nothing to store to s3 for bufferfile: %s\n", chunkBuffer.File.Name())
		}
	} else { // Write to s3 in a new filename
		alreadyExists := true
		chunkkey := S3KafkaChunkKey(chunkBuffer.Topic, chunkBuffer.Partition, chunkBuffer.InitialOffset, chunkBuffer.Offset)
		for alreadyExists {
			writeTime := time.Now()
			s3path = fmt.Sprintf("%s%s%s", S3TopicPartitionPrefix(chunkBuffer.Topic, chunkBuffer.Partition), S3DatePrefix(&writeTime), chunkkey)
			alreadyExists, err = s3bucket.Exists(s3path)
			if err != nil {
				panic(err)
				return false, err
			}
		}

		fmt.Printf("S3 Put Object: { Bucket: %s, Key: %s, MimeType:%s }\n", s3bucket.Name, s3path, mime.TypeByExtension(filepath.Ext(chunkBuffer.File.Name())))

		err = s3bucket.Put(s3path, contents, mime.TypeByExtension(filepath.Ext(chunkBuffer.File.Name())), s3.Private, s3.Options{})
		if err != nil {
			panic(err)
		}
	}

	if !keepBufferFiles {
		if debug {
			fmt.Printf("Deleting bufferfile: %s\n", chunkBuffer.File.Name())
		}
		err = os.Remove(chunkBuffer.File.Name())
		if err != nil {
			fmt.Errorf("Error deleting bufferfile %s: %#v", chunkBuffer.File.Name(), err)
		}
	}

	return true, nil
}

func LastS3KeyWithPrefix(bucket *s3.Bucket, prefix *string) (string, error) {
	narrowedPrefix := *prefix
	keyMarker := ""

	// First, do a few checks for shortcuts for checking backwards: focus in on the 14 days.
	// Otherwise just loop forward until there aren't any more results
	currentDay := time.Now()
	for i := 0; i < S3_REWIND_IN_DAYS_BEFORE_LONG_LOOP; i++ {
		testPrefix := fmt.Sprintf("%s%s", *prefix, S3DatePrefix(&currentDay))
		results, err := bucket.List(narrowedPrefix, "", keyMarker, 0)
		if err != nil && len(results.Contents) > 0 {
			narrowedPrefix = testPrefix
			break
		}
		currentDay = currentDay.Add(-1 * time.Duration(DAY_IN_SECONDS) * time.Second)
	}

	lastKey := ""
	moreResults := true
	for moreResults {
		results, err := bucket.List(narrowedPrefix, "", keyMarker, 0)
		if err != nil {
			return lastKey, err
		}

		if len(results.Contents) == 0 { // empty request, return last found lastKey
			return lastKey, nil
		}

		lastKey = results.Contents[len(results.Contents)-1].Key
		keyMarker = lastKey
		moreResults = results.IsTruncated
	}
	return lastKey, nil
}

func doBackup(s3bucket *s3.Bucket, client *sarama.Client, backupTask BackupTask) {

	fmt.Println("doing backup for", backupTask)
	bufferMaxSizeInByes, _ := config.GetInt64("default", "maxchunksizebytes")
	bufferMaxAgeInMinutes, _ := config.GetInt64("default", "maxchunkagemins")
	tempfilePath, _ := config.GetString("default", "filebufferpath")

	buffer := &ChunkBuffer{FilePath: &tempfilePath,
		MaxSizeInBytes: bufferMaxSizeInByes,
		MaxAgeInMins:   bufferMaxAgeInMinutes,
		Topic:          &backupTask.Topic,
		Partition:      backupTask.Partition,
		Offset:         backupTask.Offset,
		InitialOffset:  backupTask.Offset,
	}
	buffer.CreateBufferFileOrPanic()

	consumerConf := sarama.ConsumerConfig{MaxWaitTime: 100, OffsetMethod: sarama.OffsetMethodManual, OffsetValue: int64(backupTask.Offset)}
	consumer, err := sarama.NewConsumer(client, backupTask.Topic, int32(backupTask.Partition), "my_consumer_group", &consumerConf)
	if err != nil {
		panic(err)
	} else {
		fmt.Println("> consumer ready")
	}
	defer consumer.Close()

	msgCount := 0
consumerLoop:
	for {
		select {
		case event := <-consumer.Events():
			if event.Err != nil {
				panic(event.Err)
			}
			buffer.PutMessage(event)
			println(string(event.Value), event.Offset)

			// check for max size and max age ... if over, rotate
			// to new buffer file and upload the old one.
			if buffer.NeedsRotation() {

				if debug {
					fmt.Printf("Log Rotation needed! Rotating out of %s\n", buffer.File.Name())
				}
				buffer.StoreToS3AndRelease(s3bucket)

				buffer = &ChunkBuffer{FilePath: &tempfilePath,
					MaxSizeInBytes: bufferMaxSizeInByes,
					MaxAgeInMins:   bufferMaxAgeInMinutes,
					Topic:          &backupTask.Topic,
					Partition:      backupTask.Partition,
					Offset:         uint64(event.Offset) + 1,
					InitialOffset:  uint64(event.Offset) + 1,
				}
				buffer.CreateBufferFileOrPanic()

				if debug {
					fmt.Printf("Rotating into %s\n", buffer.File.Name())
				}

			}

			msgCount += 1
		case <-time.After(5 * time.Second):
			fmt.Println("> timed out")
			break consumerLoop
		}
	}
	fmt.Println("Got", msgCount, "messages.")
}

func init() {
	flag.StringVar(&configFilename, "c", "consumer.properties", "path to config file")
	flag.BoolVar(&keepBufferFiles, "k", false, "keep buffer files around for inspection")
	flag.BoolVar(&shouldOutputVersion, "v", false, "output the current version and quit")
	flag.StringVar(&hostsStr, "h", "localhost:9092", "host:port comma separated list")
	hosts = strings.Split(hostsStr, ",")
}

func main() {
	flag.Parse() // Read argv

	var backupTasks []BackupTask

	println("config file name", configFilename)
	var err error
	// Read configuration file
	config, err = configfile.ReadConfigFile(configFilename)
	if err != nil {
		fmt.Printf("Couldn't read config file %s because: %#v\n", configFilename, err)
		panic(err)
	}
	// host, _ := config.GetString("kafka", "host")
	debug, _ = config.GetBool("default", "debug")
	// bufferMaxSizeInByes, _ := config.GetInt64("default", "maxchunksizebytes")
	// bufferMaxAgeInMinutes, _ := config.GetInt64("default", "maxchunkagemins")
	// port, _ := config.GetString("kafka", "port")
	// hostname := fmt.Sprintf("%s:%s", host, port)
	awsKey, _ := config.GetString("s3", "accesskey")
	awsSecret, _ := config.GetString("s3", "secretkey")
	awsRegion, _ := config.GetString("s3", "region")
	s3BucketName, _ := config.GetString("s3", "bucket")

	s3bucket := s3.New(aws.Auth{AccessKey: awsKey, SecretKey: awsSecret}, aws.Regions[awsRegion]).Bucket(s3BucketName)

	client, err := sarama.NewClient("my_client", hosts, &sarama.ClientConfig{MetadataRetries: 1, WaitForElection: 250 * time.Millisecond})
	if err != nil {
		println(err.Error())
	}

	// figure out list of topicpartitions & their offsets to backup
	topics, _ := client.Topics()
	for _, topic := range topics {
		println("Topic:", topic)
		partitions, _ := client.Partitions(topic)
		for _, partition := range partitions {
			println("Partition:", partition)

			offset, err := FetchLastCommittedOffset(s3bucket, &topic, int64(partition))
			if err != nil {
				println(err.Error())
				continue
			}
			backupTask := BackupTask{Offset: offset, Topic: topic, Partition: int64(partition)}
			backupTasks = append(backupTasks, backupTask)
		}
	}

	for _, backupTask := range backupTasks {
		doBackup(s3bucket, client, backupTask)
	}
}