package s3backup

import (
	"fmt"
	"github.com/Shopify/sarama"
	"io/ioutil"
	"mime"
	"os"
	// "strconv"
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/s3"
	configfile "github.com/crowdmob/goconfig"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

var debug bool
var keepBufferFiles bool
var config *configfile.ConfigFile
var clusterId string

const (
	VERSION                            = "0.1"
	ONE_MINUTE_IN_NANOS                = 60000000000
	S3_REWIND_IN_DAYS_BEFORE_LONG_LOOP = 7
	DAY_IN_SECONDS                     = 24 * 60 * 60
)

type ChunkBuffer struct {
	File           *os.File
	FilePath       *string
	MaxAgeInMins   int64
	MaxSizeInBytes int64
	ClusterId      *string
	Topic          *string
	Partition      int64
	Offset         uint64
	InitialOffset  uint64
	expiresAt      int64
	length         int64
}

type BackupTask struct {
	ClusterId string
	Topic     string
	Partition int64
	Offset    uint64
}

type errorNoOffset struct {
}

func (e *errorNoOffset) Error() string {
	return "Cannot find offset"
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
	return chunkBuffer.length >= chunkBuffer.MaxSizeInBytes
}

func (chunkBuffer *ChunkBuffer) TooOld() bool {
	return time.Now().UnixNano() >= chunkBuffer.expiresAt
}

func (chunkBuffer *ChunkBuffer) NeedsRotation() bool {
	return chunkBuffer.TooBig() || chunkBuffer.TooOld()
}

// Look at S3 to see what was the last committed offset
func FetchLastCommittedOffset(bucket *s3.Bucket, prefix string, topic string, partition int64) (uint64, error) {
	keyMarker := ""

	// First, do a few checks for shortcuts for checking backwards: focus in on the 14 days.
	// Otherwise just loop forward until there aren't any more results
	currentDay := time.Now()
	for i := 0; i < S3_REWIND_IN_DAYS_BEFORE_LONG_LOOP; i++ {
		testPrefix := fmt.Sprintf("%s%s", prefix, S3DatePrefix(&currentDay))
		results, err := bucket.List(testPrefix, "", keyMarker, 0)
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
	foundOffset := false
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

		offset = DetermineOffset(bucket, lastKey, topic, partition)
		foundOffset = true
	}

	if foundOffset {
		return offset, nil
	} else {
		return offset, &errorNoOffset{}
	}
}

// Figure out what offset to start backing up for
func FetchOffsetToBackup(bucket *s3.Bucket, prefix string, topic string, partition int64) (uint64, error) {
	nobackupYet := &errorNoOffset{}
	offset, err := FetchLastCommittedOffset(bucket, prefix, topic, partition)
	if err != nil && err.Error() == nobackupYet.Error() {
		return 0, nil
	} else if err == nil {
		return offset + 1, nil
	} else {
		return 0, err
	}
}

// Given an s3 obj, find out the last offset inside it
func DetermineOffset(s3bucket *s3.Bucket, latestKey string, topic string, partition int64) (offset uint64) {
	contentBytes, err := s3bucket.Get(latestKey)
	guidPrefix := KafkaMsgGuidPrefix(&topic, partition)
	lines := strings.Split(string(contentBytes), "\n")
	for l := len(lines) - 1; l >= 0; l-- {
		if debug {
			fmt.Printf("    Looking at Line '%s'\n", lines[l])
		}
		if strings.HasPrefix(lines[l], guidPrefix) { // found a line with a guid, extract offset and escape out
			guidSplits := strings.SplitN(strings.SplitN(lines[l], "|", 2)[0], guidPrefix, 2)
			offsetString := guidSplits[len(guidSplits)-1]
			offset, err = strconv.ParseUint(offsetString, 10, 64)
			if err != nil {
				panic(err)
			}
			break
		}
	}
	return
}
func ParseS3KeyForOffsets(key string) (startOffset uint64, endOffset uint64) {
	baseName := path.Base(key)
	println("Splitting", baseName)
	tokens := strings.Split(baseName, "_")
	var expectedTokens int
	if clusterId != "" {
		expectedTokens = 5
	} else {
		expectedTokens = 4
	}

	if len(tokens) != expectedTokens {
		return
	}

	fmt.Println(tokens[expectedTokens-2], tokens[expectedTokens-1])
	startOffset, _ = strconv.ParseUint(tokens[expectedTokens-2], 10, 64)
	endOffset, _ = strconv.ParseUint(tokens[expectedTokens-1], 10, 64)
	return
}

func S3DatePrefix(t *time.Time) string {
	return fmt.Sprintf("%d/%d/%d/", t.Year(), t.Month(), t.Day())
}

func S3TopicPartitionPrefix(clusterId string, topic string, partition int64) string {
	if clusterId != "" {
		return fmt.Sprintf("%s/%s/%d/", clusterId, topic, partition)
	} else {
		return fmt.Sprintf("%s/%d/", topic, partition)
	}
}

func (this *ChunkBuffer) S3TopicPartitionPrefix() string {
	return S3TopicPartitionPrefix(*this.ClusterId, *this.Topic, this.Partition)
}

func (this *ChunkBuffer) KafkaMsgGuidPrefix() string {
	return fmt.Sprintf("t_%s-p_%d-o_", *this.Topic, this.Partition)
}

func KafkaMsgGuidPrefix(topic *string, partition int64) string {
	return fmt.Sprintf("t_%s-p_%d-o_", *topic, partition)
}

func (this *ChunkBuffer) S3KafkaChunkKey() string {
	if *this.ClusterId != "" {
		return fmt.Sprintf("%s_%s_%d_%d_%d", *this.ClusterId, *this.Topic, this.Partition, this.InitialOffset, this.Offset)
	} else {
		return fmt.Sprintf("%s_%d_%d_%d", *this.Topic, this.Partition, this.InitialOffset, this.Offset)
	}
}

func (chunkBuffer *ChunkBuffer) PutMessage(msg *sarama.ConsumerEvent) {
	uuid := []byte(fmt.Sprintf("%s%d|", chunkBuffer.KafkaMsgGuidPrefix(), msg.Offset))
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
		chunkkey := chunkBuffer.S3KafkaChunkKey()
		for alreadyExists {
			writeTime := time.Now()
			s3Prefix := chunkBuffer.S3TopicPartitionPrefix()
			// s3path = fmt.Sprintf("%s%s%s", s3Prefix, S3DatePrefix(&writeTime), chunkkey)
			println("Writing out to", s3Prefix)
			s3path = fmt.Sprintf("%s%s%d-%s", s3Prefix, S3DatePrefix(&writeTime), writeTime.UnixNano(), chunkkey)
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

// func LastS3KeyWithPrefix(bucket *s3.Bucket, prefix *string) (string, error) {
// 	narrowedPrefix := *prefix
// 	keyMarker := ""

// 	// First, do a few checks for shortcuts for checking backwards: focus in on the 14 days.
// 	// Otherwise just loop forward until there aren't any more results
// 	currentDay := time.Now()
// 	for i := 0; i < S3_REWIND_IN_DAYS_BEFORE_LONG_LOOP; i++ {
// 		testPrefix := fmt.Sprintf("%s%s", *prefix, S3DatePrefix(&currentDay))
// 		results, err := bucket.List(narrowedPrefix, "", keyMarker, 0)
// 		if err != nil && len(results.Contents) > 0 {
// 			narrowedPrefix = testPrefix
// 			break
// 		}
// 		currentDay = currentDay.Add(-1 * time.Duration(DAY_IN_SECONDS) * time.Second)
// 	}

// 	lastKey := ""
// 	moreResults := true
// 	for moreResults {
// 		results, err := bucket.List(narrowedPrefix, "", keyMarker, 0)
// 		if err != nil {
// 			return lastKey, err
// 		}

// 		if len(results.Contents) == 0 { // empty request, return last found lastKey
// 			return lastKey, nil
// 		}

// 		lastKey = results.Contents[len(results.Contents)-1].Key
// 		keyMarker = lastKey
// 		moreResults = results.IsTruncated
// 	}
// 	return lastKey, nil
// }

func doBackup(s3bucket *s3.Bucket, client *sarama.Client, backupTask BackupTask) {
	fmt.Println("doing backup for", backupTask)
	bufferMaxSizeInByes, _ := config.GetInt64("default", "maxchunksizebytes")
	bufferMaxAgeInMinutes, _ := config.GetInt64("default", "maxchunkagemins")
	tempfilePath, _ := config.GetString("default", "filebufferpath")

	// create tempfilePath if it's not there
	if _, err := os.Stat(tempfilePath); err != nil {
		os.MkdirAll(tempfilePath, 0700)
	}

	buffer := &ChunkBuffer{FilePath: &tempfilePath,
		MaxSizeInBytes: bufferMaxSizeInByes,
		MaxAgeInMins:   bufferMaxAgeInMinutes,
		ClusterId:      &backupTask.ClusterId,
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
					ClusterId:      &backupTask.ClusterId,
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

func topicNeedsBackup(topicsToBackup []string, topic string) bool {
	if len(topicsToBackup) < 1 {
		return false
	} else if topicsToBackup[0] == "*" {
		return true
	}

	for _, t := range topicsToBackup {
		if t == topic {
			return true
		}
	}
	return false
}

func Backup(p_config *configfile.ConfigFile, p_clusterId string, hosts []string, topicsToBackup []string) {
	var backupTasks []BackupTask
	var err error
	config = p_config
	clusterId = p_clusterId

	debug, _ = config.GetBool("default", "debug")
	awsKey, _ := config.GetString("s3", "accesskey")
	awsSecret, _ := config.GetString("s3", "secretkey")
	awsRegion, _ := config.GetString("s3", "region")
	s3BucketName, _ := config.GetString("s3", "bucket")

	s3bucket := s3.New(aws.Auth{AccessKey: awsKey, SecretKey: awsSecret}, aws.Regions[awsRegion]).Bucket(s3BucketName)

	client, err := sarama.NewClient("my_client", hosts, &sarama.ClientConfig{MetadataRetries: 1, WaitForElection: 250 * time.Millisecond})
	if err != nil {
		println(err.Error())
	}

	// figure out list of topicpartitions & what starting offset we need to backup
	// create list of backup tasks
	topics, _ := client.Topics()
	for _, topic := range topics {
		println("Topic:", topic)

		if !topicNeedsBackup(topicsToBackup, topic) {
			println("Skipping", topic, "because it does not need to be backed up")
			continue
		}
		partitions, _ := client.Partitions(topic)
		for _, partition := range partitions {
			println("Partition:", partition)

			s3Prefix := S3TopicPartitionPrefix(clusterId, topic, int64(partition))
			offset, err := FetchOffsetToBackup(s3bucket, s3Prefix, topic, int64(partition))
			println("OFFSET is", offset)
			if err != nil {
				println(err.Error())
				continue
			}
			backupTask := BackupTask{Offset: offset, ClusterId: clusterId, Topic: topic, Partition: int64(partition)}
			backupTasks = append(backupTasks, backupTask)
		}
	}

	// perform the backup tasks
	for _, backupTask := range backupTasks {
		doBackup(s3bucket, client, backupTask)
	}
}
