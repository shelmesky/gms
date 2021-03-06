package server

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/shelmesky/gms/server/common"
	"go.etcd.io/etcd/clientv3"
	"log"
)

func CreateTopicOnEtcd(topicName string, partitionCount, replicaCount, inSyncReplicas uint32) error {
	var err error
	var client *clientv3.Client

	client, err = clientv3.New(clientv3.Config{
		Endpoints: []string{common.GlobalConfig.EtcdServer},
	})

	if err != nil {
		return fmt.Errorf("%s: connect to kv server failed\n", err)
	}

	kv := clientv3.NewKV(client)

	key := fmt.Sprintf("/topics/%s", topicName)
	value := makeTopicInfo(topicName, partitionCount, replicaCount, inSyncReplicas)

	getResp, err := kv.Get(context.Background(), key)

	if err != nil {
		return fmt.Errorf("%s: get %s from etcd failed\n", err, key)
	}

	// 如果etcd已经存topic目录则返回错误
	if len(getResp.Kvs) > 0 {
		return fmt.Errorf("key %s already exits!\n", key)
	}

	// 将topic的基本信息保存到etcd
	putResp, err := kv.Put(context.Background(), key, value)

	if err != nil {
		return fmt.Errorf("%s: kv put failed\n", err)
	}
	log.Printf("set topics info: %s, %v\n", value, putResp)

	return nil
}

func makeTopicInfo(topicName string, partitionCount, replicaCount, inSyncReplicas uint32) string {
	var topic common.Topic
	topic.TopicName = topicName
	topic.PartitionCount = partitionCount
	topic.ReplicaCount = replicaCount
	topic.InSyncReplicas = inSyncReplicas

	jsonBytes, err := json.Marshal(topic)
	if err != nil {
		return ""
	}

	return string(jsonBytes)
}
