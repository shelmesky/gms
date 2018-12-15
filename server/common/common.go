package common

import (
	"unsafe"
)

const (
	REQUEST_LEN    = 24
	MESSAGE_LEN    = 40
	READ_BUF_SIZE  = 4096
	WRITE_BUF_SIZE = 4096
)

type Request struct {
	TotalLength    uint64
	Version        uint16
	Sequence       uint32
	MetaDataLength uint32
	BodyLength     uint32
}

type MessageType struct {
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
