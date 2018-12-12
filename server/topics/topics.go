package topics

import (
	"github.com/shelmesky/gms/server/partition"
	"io/ioutil"
)

func CreateTopic(topicName string, numPartitions int) error {
	return partition.CreatePartitionList(topicName, numPartitions)
}

type Topics struct {
	numTopics int
	topicMap  map[string]partition.PartitionList
}

func (topics Topics) Init() error {
	topics.topicMap = make(map[string]partition.PartitionList, 16)

	dirs, err := ioutil.ReadDir(".")
	if err != nil {
		return err
	}

	for idx := range dirs {
		dir := dirs[idx]
		if dir.IsDir() {
			topicName := dir.Name()

			var partitionList partition.PartitionList
			err = partitionList.Init(topicName)
			if err != nil {
				return err
			}

			topics.topicMap[topicName] = partitionList
			topics.numTopics += 1
		}
	}

	return nil
}

func (topics Topics) GetTopic(topicName string) *partition.PartitionList {
		if value, ok := topics.topicMap[topicName]; ok {
			return &value
		}

		return nil
}

func (topics Topics) SetTopic(topicName string, topic partition.PartitionList) {
	topics.topicMap[topicName] = topic
}
