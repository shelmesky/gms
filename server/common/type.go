package common

// 在etcd中保存的topic信息
type Topic struct {
	TopicName      string `json:"topic_name"`
	PartitionCount uint32 `json:"partition_count"`
	ReplicaCount   uint32 `json:"replica_count"`
}