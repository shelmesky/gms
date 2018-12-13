package common

import (
	pb "github.com/shelmesky/gms/server/protobuf"
	"unsafe"
)

type Slice struct {
	addr uintptr
	len  int
	cap  int
}


func BytesToMessage(data []byte) *pb.MessageType {
	var m *pb.MessageType = *(**pb.MessageType)(unsafe.Pointer(&data))
	return m
}

func MessageToBytes(message *pb.MessageType) []byte {
	length := unsafe.Sizeof(*message)
	bytes := &Slice{
		addr: uintptr(unsafe.Pointer(message)),
		cap:  int(length),
		len:  int(length),
	}
	data := *(*[]byte)(unsafe.Pointer(bytes))
	return data
}