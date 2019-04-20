package topics

import (
	"github.com/shelmesky/gms/server/common"
	"github.com/shelmesky/gms/server/partition"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"os"
)

var (
	TopicManager *Topics
)

func Init() {
	if err := os.Chdir(common.GlobalConfig.DataDir); err != nil {
		log.Errorln(err)
		os.Exit(1)
	}

	var t Topics
	err := t.Init()
	if err != nil {
		panic(err.Error())
	}

	TopicManager = &t
}

func CreateTopic(topicName string, numPartitions int) error {
	return partition.CreatePartitionList(topicName, numPartitions)
}

type Topics struct {
	NumTopics int
	TopicMap  map[string]partition.PartitionList
}

func (topics *Topics) Init() error {
	topics.TopicMap = make(map[string]partition.PartitionList, 16)

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

			topics.TopicMap[topicName] = partitionList
			topics.NumTopics += 1

			partitionList.StartWorker()
		}
	}

	return nil
}

func (topics *Topics) GetTopic(topicName string) *partition.PartitionList {
		if value, ok := topics.TopicMap[topicName]; ok {
			return &value
		}

		return nil
}

func (topics *Topics) SetTopic(topicName string, topic partition.PartitionList) {
	topics.TopicMap[topicName] = topic
}
