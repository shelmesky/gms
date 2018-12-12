package common

import "unsafe"

// 消息格式
type Message struct {
	CRC32        int    // crc32校验
	Magic        byte   // magic数字
	Attributes   byte   // 属性组合
	KeyLength    int    // key的长度
	KeyPayload   []byte // key的内容
	ValueLength  int    // value的长度
	ValuePayload []byte // value的内容
}

type Slice struct {
	addr uintptr
	len  int
	cap  int
}

func (m *Message) Bytes() []byte {
	length := unsafe.Sizeof(m)
	bytes := &Slice{
		addr: uintptr(unsafe.Pointer(m)),
		cap:  int(length),
		len:  int(length),
	}
	data := *(*[]byte)(unsafe.Pointer(bytes))
	return data
}

// 生产者写请求
type WriteRequest struct {
	Topic     string
	Partition int
	Message   Message
}

// 消费者读请求
type ReadRequest struct {
	Topic       string
	Partition   int
	StartOffset int
	EndOffset   int
	ReadSize    int
}
