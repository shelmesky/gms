package common

import (
	"unsafe"
)

const (
	REQUEST_LEN       = 24
	MESSAGE_LEN       = 40
	READ_BUF_SIZE     = 4096
	WRITE_BUF_SIZE    = 4096
	TOPIC_NAME_LEN    = 128
	PARTITION_NUM_LEN = 128
)

const (
	Write = 1 // 写入类消息
	Read  = 2 // 读取类消息
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

// 每个消息都有的请求头部
type Request struct {
	TotalLength    uint64
	Version        uint16
	Sequence       uint32
	MetaDataLength uint32
	BodyLength     uint32
}

// 当Request.BodyLength 不等于 MessageType.Length
// 说明消息是批量发送的

// 写入到磁盘的消息结构
type MessageType struct {
	Length      uint64
	CRC32       uint32
	Magic       uint32
	Attributes  uint32
	KeyLength   uint64
	ValueLength uint64
}

type Slice struct {
	addr uintptr
	len  int
	cap  int
}

func BytesToMessage(data []byte) *MessageType {
	var m *MessageType = *(**MessageType)(unsafe.Pointer(&data))
	return m
}

func MessageToBytes(message *MessageType) []byte {
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
