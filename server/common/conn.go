package common

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math"
	"net"
)

type SocketBuffer struct {
	conn   *net.TCPConn
	buffer []byte
	size   int
	in     int
	out    int
}

func NewSocketBuffer(bufferSize int, conn *net.TCPConn) SocketBuffer {
	var socketBuffer SocketBuffer
	socketBuffer.buffer = make([]byte, bufferSize, bufferSize)
	socketBuffer.size = bufferSize
	socketBuffer.in = 0
	socketBuffer.out = 0
	socketBuffer.conn = conn
	return socketBuffer
}

func (b *SocketBuffer) ReadFromSocket() int {
	remain := b.Remain()
	buffer := make([]byte, remain)
	n, err := b.conn.Read(buffer)
	if err == io.EOF {
		return 0
	}
	if err != nil {
		return 0
	}
	if n == 0 {
		return 0
	}
	log.Printf("read %d bytes from socket\n", n)
	b.WriteBytes(buffer[:n], n)

	return n
}

func (b *SocketBuffer) ReadBytes(buffer []byte, size int) int {
	n, err := io.ReadFull(b.conn, buffer)
	if err != nil {
		return 0
	}
	return n

	/*
		originSize := size
		n := 0
		for {
			length := 0
			size = int(math.Min(float64(size), float64(b.in-b.out)))
			length = int(math.Min(float64(size), float64(b.size-(b.out&(b.size-1)))))
			start := b.out & (b.size - 1)
			// TODO: 性能优化
			// 当ring buffer中的数据长度满足需求, 且是连续的(没有回绕)
			// 应该直接返回ring buffer的slice切片, 而不是copy
			copy(buffer, b.buffer[start:start+length])
			copy(buffer[length:], b.buffer[0:size-length])
			b.out += size
			originSize -= size
			n += size
			if originSize > 0 {
				size = originSize
				readN := b.ReadFromSocket()
				if readN == 0 {
					break
				}
			} else {
				break
			}
		}
		return n

	*/
}

func (b *SocketBuffer) WriteBytes(buffer []byte, size int) int {
	log.Printf("write %d bytes to ring buffer\n", len(buffer))
	if size > b.size {
		return 0
	}
	length := 0
	size = int(math.Min(float64(size), float64(b.size-b.in+b.out)))
	length = int(math.Min(float64(size), float64(b.size-(b.in&(b.size-1)))))
	start := b.in & (b.size - 1)
	copy(b.buffer[start:start+length], buffer)
	if (size - length) >= 1 {
		copy(b.buffer[size-length-1:], buffer[length:])
	}
	b.in += size
	return size
}

func (b *SocketBuffer) ReadUint32() (uint32, error) {
	needSize := 4
	buffer := make([]byte, needSize)
	length := b.ReadBytes(buffer, needSize)
	if length == 0 {
		return 0, fmt.Errorf("read zero length")
	}

	if length < needSize {
		return uint32(length), fmt.Errorf("read length [%d] smaller than %d\n", length, needSize)
	}

	return binary.LittleEndian.Uint32(buffer), nil
}

func (b *SocketBuffer) ReadUint64() (uint64, error) {
	needSize := 8
	buffer := make([]byte, needSize)
	length := b.ReadBytes(buffer, needSize)
	if length == 0 {
		return 0, fmt.Errorf("read zero length")
	}

	if length < needSize {
		return uint64(length), fmt.Errorf("read length [%d] smaller than %d\n", length, needSize)
	}

	return binary.LittleEndian.Uint64(buffer), nil
}

func (b *SocketBuffer) Remain() int {
	return b.size - (b.in - b.out)
}

type Client struct {
	Conn        *net.TCPConn
	SockFD      int
	Alive       bool
	ReadBuffer  SocketBuffer
	WriteBuffer SocketBuffer
}

func NewClient(conn *net.TCPConn) Client {
	var client Client
	client.Conn = conn
	client.ReadBuffer = NewSocketBuffer(READ_BUF_SIZE, conn)
	client.WriteBuffer = NewSocketBuffer(WRITE_BUF_SIZE, conn)
	return client
}
