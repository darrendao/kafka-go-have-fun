package main

import (
	"flag"
	"fmt"
	configfile "github.com/crowdmob/goconfig"
	"github.com/darrendao/kafka-go-have-fun/s3backup"
	"os"
	"strings"
)

var configFilename string
var keepBufferFiles bool
var debug bool
var shouldOutputVersion bool
var hostsStr string
var config *configfile.ConfigFile
var clusterId string
var topicsStr string

const (
	VERSION                            = "0.1"
	ONE_MINUTE_IN_NANOS                = 60000000000
	S3_REWIND_IN_DAYS_BEFORE_LONG_LOOP = 3
	DAY_IN_SECONDS                     = 24 * 60 * 60
)

func init() {
	flag.StringVar(&configFilename, "c", "consumer.properties", "path to config file")
	flag.BoolVar(&keepBufferFiles, "k", false, "keep buffer files around for inspection")
	flag.BoolVar(&shouldOutputVersion, "v", false, "output the current version and quit")
	flag.StringVar(&hostsStr, "h", "localhost:9092", "host:port comma separated list")
	flag.StringVar(&clusterId, "i", "", "ID of the Kafka cluster")
	flag.StringVar(&topicsStr, "t", "*", "comma separated list of topics. Defaults to all.")
}

func main() {
	flag.Parse() // Read argv

	hosts := strings.Split(hostsStr, ",")
	topicsToBackup := strings.Split(topicsStr, ",")

	if shouldOutputVersion {
		fmt.Printf("kafka-s3-consumer %s\n", VERSION)
		os.Exit(0)
	}

	// Read configuration file
	var err error
	config, err = configfile.ReadConfigFile(configFilename)
	if err != nil {
		fmt.Printf("Couldn't read config file %s because: %#v\n", configFilename, err)
		panic(err)
	}
	println(hosts, topicsToBackup)
	s3backup.Backup(config, clusterId, hosts, topicsToBackup)

	// var backupTasks []BackupTask
	// debug, _ = config.GetBool("default", "debug")
	// awsKey, _ := config.GetString("s3", "accesskey")
	// awsSecret, _ := config.GetString("s3", "secretkey")
	// awsRegion, _ := config.GetString("s3", "region")
	// s3BucketName, _ := config.GetString("s3", "bucket")

	// s3bucket := s3.New(aws.Auth{AccessKey: awsKey, SecretKey: awsSecret}, aws.Regions[awsRegion]).Bucket(s3BucketName)

	// client, err := sarama.NewClient("my_client", hosts, &sarama.ClientConfig{MetadataRetries: 1, WaitForElection: 250 * time.Millisecond})
	// if err != nil {
	// 	println(err.Error())
	// }

	// // figure out list of topicpartitions & their last backed up offset
	// // create list of backup tasks
	// topics, _ := client.Topics()
	// for _, topic := range topics {
	// 	println("Topic:", topic)

	// 	if !topicNeedsBackup(topicsToBackup, topic) {
	// 		println("Skipping", topic, "because it does not need to be backed up")
	// 		continue
	// 	}
	// 	partitions, _ := client.Partitions(topic)
	// 	for _, partition := range partitions {
	// 		println("Partition:", partition)

	// 		s3Prefix := S3TopicPartitionPrefix(clusterId, topic, int64(partition))
	// 		offset, err := FetchLastCommittedOffset(s3bucket, s3Prefix)
	// 		if err != nil {
	// 			println(err.Error())
	// 			continue
	// 		}
	// 		backupTask := BackupTask{Offset: offset, ClusterId: clusterId, Topic: topic, Partition: int64(partition)}
	// 		backupTasks = append(backupTasks, backupTask)
	// 	}
	// }

	// // perform the backup tasks
	// for _, backupTask := range backupTasks {
	// 	doBackup(s3bucket, client, backupTask)
	// }
}
