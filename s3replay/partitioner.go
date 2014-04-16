package s3replay

import (
	"github.com/Shopify/sarama"
	"strconv"
	"sync"
)

type KaasPartitionEncoder struct {
	myPartition string
}

func (k KaasPartitionEncoder) Encode() ([]byte, error) {
	return []byte(k.myPartition), nil
}

type ExactPartitioner struct {
	m sync.Mutex
}

func NewExactPartitioner() *ExactPartitioner {
	p := new(ExactPartitioner)
	return p
}

func (p *ExactPartitioner) Partition(key sarama.Encoder, numPartitions int32) int32 {
	p.m.Lock()
	defer p.m.Unlock()
	myKey, _ := key.Encode()

	partition, _ := strconv.Atoi(string(myKey))
	return int32(partition)
}
