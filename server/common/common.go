package common

import (
	"unsafe"
)

const (
	REQUEST_LEN       = 24   // Request结构的长度
	WRITE_MESSAGE_LEN = 40   // 写入型消息的头部长度
	READ_BUF_SIZE     = 4096 // RingBuffer读取缓冲区大小
	WRITE_BUF_SIZE    = 4096 // RingBuffer写入缓冲区大小
	TOPIC_NAME_LEN    = 128  // topic名字允许的最大长度
	PARTITION_NUM_LEN = 128  // partition分区号允许的最大长度
)

const (
	Write       = 1 // 写入类消息
	Read        = 2 // 读取类消息
	CreateTopic = 3 // 创建topic
)

// 保存在meta中, 作为处理body的辅助信息
// 客户端消息写入命令
type WriteMessageAction struct {
	Action          uint32
	TopicName       [128]byte
	PartitionNumber [32]byte
}

func BytesToWriteMessageAction(data []byte) *WriteMessageAction {
	var act *WriteMessageAction = *(**WriteMessageAction)(unsafe.Pointer(&data))
	return act
}

func WriteMessageActionToBytes(action *WriteMessageAction) []byte {
	length := unsafe.Sizeof(*action)
	b := &Slice{
		addr: uintptr(unsafe.Pointer(action)),
		cap:  int(length),
		len:  int(length),
	}
	data := *(*[]byte)(unsafe.Pointer(b))
	return data
}

func NewWriteMessageAction(topicName, PartitionNumber string) []byte {
	var writeMessageAction WriteMessageAction
	writeMessageAction.Action = Write

	if len(topicName) > TOPIC_NAME_LEN {
		panic("topic name is too large")
	}
	for i := 0; i < len(topicName); i++ {
		writeMessageAction.TopicName[i] = topicName[i]
	}

	if len(PartitionNumber) > PARTITION_NUM_LEN {
		panic("partition mum is too large")
	}
	for i := 0; i < len(PartitionNumber); i++ {
		writeMessageAction.PartitionNumber[i] = PartitionNumber[i]
	}
	return WriteMessageActionToBytes(&writeMessageAction)
}

// 客户端读取消息命令
type ReadMessageAction struct {
	Action          uint32
	TopicName       [128]byte
	PartitionNumber [32]byte
	TargetOffset    uint32
	Count           uint32
}

func BytesToReadMessageAction(data []byte) *ReadMessageAction {
	var act *ReadMessageAction = *(**ReadMessageAction)(unsafe.Pointer(&data))
	return act
}

func ReadMessageActionToBytes(action *ReadMessageAction) []byte {
	length := unsafe.Sizeof(*action)
	b := &Slice{
		addr: uintptr(unsafe.Pointer(action)),
		cap:  int(length),
		len:  int(length),
	}
	data := *(*[]byte)(unsafe.Pointer(b))
	return data
}

func NewReadMessageAction(topicName, PartitionNumber string, targetOffset, count uint32) []byte {
	var readMessageAction ReadMessageAction
	readMessageAction.Action = Read

	if len(topicName) > TOPIC_NAME_LEN {
		panic("topic name is too large")
	}
	for i := 0; i < len(topicName); i++ {
		readMessageAction.TopicName[i] = topicName[i]
	}

	if len(PartitionNumber) > PARTITION_NUM_LEN {
		panic("partition mum is too large")
	}
	for i := 0; i < len(PartitionNumber); i++ {
		readMessageAction.PartitionNumber[i] = PartitionNumber[i]
	}

	readMessageAction.TargetOffset = targetOffset
	readMessageAction.Count = count

	return ReadMessageActionToBytes(&readMessageAction)
}

// 创建topic请求
type CreateTopicAction struct {
	Action         uint32
	TopicName      [128]byte
	PartitionCount uint32
	ReplicaCount   uint32
}

func BytesToCreateTopicAction(data []byte) *CreateTopicAction {
	var act *CreateTopicAction = *(**CreateTopicAction)(unsafe.Pointer(&data))
	return act
}

func CreateTopicActionToBytes(action *CreateTopicAction) []byte {
	length := unsafe.Sizeof(*action)
	b := &Slice{
		addr: uintptr(unsafe.Pointer(action)),
		cap:  int(length),
		len:  int(length),
	}
	data := *(*[]byte)(unsafe.Pointer(b))
	return data
}

func NewCreateTopicAction(topicName string, PartitionCount, ReplicaCount uint32) []byte {
	var action CreateTopicAction
	action.Action = CreateTopic

	if len(topicName) > TOPIC_NAME_LEN {
		panic("topic name is too large")
	}
	for i := 0; i < len(topicName); i++ {
		action.TopicName[i] = topicName[i]
	}

	action.PartitionCount = PartitionCount
	action.ReplicaCount = ReplicaCount

	return CreateTopicActionToBytes(&action)
}

// 每个消息都有的请求头部
type Request struct {
	TotalLength    uint64 // 消息的总长度
	Version        uint16 // 版本号
	Sequence       uint32 // 序列号
	MetaDataLength uint32 // 元数据
	BodyLength     uint32 // 消息主题长度
}

// 当Request.BodyLength 不等于 MessageType.Length
// 说明消息是批量发送的

// 写入到磁盘的消息结构
type WriteMessageType struct {
	Length      uint64 // 消息长度
	CRC32       uint32 // CRC32
	Magic       uint32 // 魔法数字
	Attributes  uint32 // 属性
	KeyLength   uint64 // KEY的长度
	ValueLength uint64 // VALUE的长度
}

type Slice struct {
	addr uintptr
	len  int
	cap  int
}

func BytesToMessage(data []byte) *WriteMessageType {
	var m *WriteMessageType = *(**WriteMessageType)(unsafe.Pointer(&data))
	return m
}

func MessageToBytes(message *WriteMessageType) []byte {
	length := unsafe.Sizeof(*message)
	bytes := &Slice{
		addr: uintptr(unsafe.Pointer(message)),
		cap:  int(length),
		len:  int(length),
	}
	data := *(*[]byte)(unsafe.Pointer(bytes))
	return data
}

func BytesToRequest(data []byte, length int) *Request {
	var r *Request = *(**Request)(unsafe.Pointer(&data))
	return r
}

func RequestToBytes(request *Request) []byte {
	length := unsafe.Sizeof(*request)
	bytes := &Slice{
		addr: uintptr(unsafe.Pointer(request)),
		cap:  int(length),
		len:  int(length),
	}
	data := *(*[]byte)(unsafe.Pointer(bytes))
	return data
}
