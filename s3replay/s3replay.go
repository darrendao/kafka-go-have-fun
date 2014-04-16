package s3replay

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/s3"
	configfile "github.com/crowdmob/goconfig"
	"github.com/darrendao/kafka-go-have-fun/s3backup"
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var config *configfile.ConfigFile

const (
	ONE_MINUTE_IN_NANOS = 60000000000
	DAY_IN_SECONDS      = 24 * 60 * 60
)

func Replay(p_config *configfile.ConfigFile, targets []string, clusterId string, topic string, partition int, startDate time.Time, endDate time.Time) {
	config = p_config
	s3keys := getS3Keys(clusterId, topic, partition, startDate, endDate)
	awsKey, _ := config.GetString("s3", "accesskey")
	awsSecret, _ := config.GetString("s3", "secretkey")
	awsRegion, _ := config.GetString("s3", "region")
	s3BucketName, _ := config.GetString("s3", "bucket")
	s3bucket := s3.New(aws.Auth{AccessKey: awsKey, SecretKey: awsSecret}, aws.Regions[awsRegion]).Bucket(s3BucketName)

	partitionKey := KaasPartitionEncoder{myPartition: strconv.Itoa(partition)}

	client, err := sarama.NewClient("client_id", targets, &sarama.ClientConfig{MetadataRetries: 1, WaitForElection: 250 * time.Millisecond})
	if err != nil {
		panic(err)
	} else {
		fmt.Println("> connected")
	}
	defer client.Close()

	partitioner := NewExactPartitioner()
	producer, err := sarama.NewProducer(client, &sarama.ProducerConfig{Partitioner: partitioner, RequiredAcks: sarama.WaitForLocal, MaxBufferedBytes: 1024, MaxBufferTime: 1})
	if err != nil {
		panic(err)
	}
	defer producer.Close()

	// TODO: we can parallelize at the partition level
	for _, s3key := range s3keys {
		replayS3Obj(producer, s3bucket, s3key, topic, partitionKey)
	}
}

func replayS3Obj(producer *sarama.Producer, s3bucket *s3.Bucket, s3key string, topic string, partitionKey KaasPartitionEncoder) {
	println("Replaying ", s3key)
	reader, _ := s3bucket.GetReader(s3key)

	offsetBuf := make([]byte, 8)
	msgLenBuf := make([]byte, 8)
	for {
		err := readMetaData(reader, offsetBuf, msgLenBuf)
		if err != nil && err == io.EOF {
			break // done!
		}

		offset := read_uint64(offsetBuf)
		msgLen := read_uint64(msgLenBuf)

		if offset < 0 {
			println("bad offset")
			break
		}

		// println("offset:", offset, "| msgLen:", msgLen)
		// fmt.Printf("in hex %x %x\n", metaDataBuf[0:7], metaDataBuf[8:15])

		buf := readMsg(reader, msgLen)
		err = producer.SendMessage(topic, partitionKey, sarama.ByteEncoder(buf))
		if err != nil {
			panic(err)
		}
	}
}

func readMetaData(reader io.Reader, offsetBuf, msgLenBuf []byte) error {
	_, err := reader.Read(offsetBuf)
	if err != nil && err != io.EOF {
		panic(err)
	}
	if err == io.EOF {
		return err
	}
	_, err = reader.Read(msgLenBuf)
	if err != nil && err != io.EOF {
		panic(err)
	}
	if err == io.EOF {
		return err
	}
	return nil
}

func readMsg(reader io.Reader, msgLen uint64) []byte {
	buf := make([]byte, msgLen)

	n, err := reader.Read(buf)
	if n != int(msgLen) {
		panic("couldn't parse data correctly")
	}
	if err != nil {
		panic(err.Error())
	}
	return buf
}

func oldreplayS3Obj(producer *sarama.Producer, s3bucket *s3.Bucket, s3key string, topic string, partitionKey KaasPartitionEncoder) {
	contentBytes, err := s3bucket.Get(s3key)
	if err != nil {
		println(err.Error())
		return
	}
	lines := strings.Split(string(contentBytes), "\n")
	var data []byte = nil
	for _, line := range lines {
		// parse line to get actual data, ignoring the first token, which is metadata for topic id, offset, etc
		if line != "" {
			validKaasPrefix := regexp.MustCompile(`t_\w+-p_\d+-o_\d+\|`)
			if validKaasPrefix.MatchString(line) {
				if data != nil {
					err := producer.SendMessage(topic, partitionKey, sarama.ByteEncoder(data))
					if err != nil {
						panic(err.Error())
					}
				}
				tokens := strings.SplitN(line, "|", 2)
				data = []byte(tokens[1])
			} else {
				data = append(data, []byte("\n")...)
				data = append(data, []byte(line)...)
			}
		}
	}
	if data != nil {
		err := producer.SendMessage(topic, partitionKey, sarama.ByteEncoder(data))
		if err != nil {
			panic(err.Error())
		}
	}
}

// TODO: this should query from kaas api
// Or authenticate against kaas to get temp token that only has access to the right dir
// and then find the files ourselves
func getS3Keys(clusterId, topic string, partition int, startDate time.Time, endDate time.Time) (s3keys []string) {

	// List s3 keys
	topicPartitionPrefix := s3backup.S3TopicPartitionPrefix(clusterId, topic, int64(partition))

	for endDate.After(startDate) || endDate.Equal(startDate) {
		moreS3Keys := getS3KeysForDate(topicPartitionPrefix, startDate)
		s3keys = append(s3keys, moreS3Keys...)
		startDate = startDate.Add(time.Duration(DAY_IN_SECONDS) * time.Second)
	}
	return
}

func getS3KeysForDate(topicPartitionPrefix string, date time.Time) (s3keys []string) {
	awsKey, _ := config.GetString("s3", "accesskey")
	awsSecret, _ := config.GetString("s3", "secretkey")
	awsRegion, _ := config.GetString("s3", "region")
	s3BucketName, _ := config.GetString("s3", "bucket")
	s3bucket := s3.New(aws.Auth{AccessKey: awsKey, SecretKey: awsSecret}, aws.Regions[awsRegion]).Bucket(s3BucketName)

	s3Prefix := fmt.Sprintf("%s%s", topicPartitionPrefix, s3backup.S3DatePrefix(&date))
	println(s3Prefix)
	keyMarker := ""
	moreResults := true
	for moreResults {
		results, _ := s3bucket.List(s3Prefix, "", keyMarker, 0)

		if len(results.Contents) == 0 { // empty request, done
			break
		}

		for _, content := range results.Contents {
			s3keys = append(s3keys, content.Key)
		}

		keyMarker = results.Contents[len(results.Contents)-1].Key
		moreResults = results.IsTruncated
	}
	return
}

func read_uint64(data []byte) (ret uint64) {
	buf := bytes.NewBuffer(data)
	binary.Read(buf, binary.LittleEndian, &ret)
	return
}
