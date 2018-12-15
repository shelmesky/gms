package server

import (
	"encoding/binary"
	"fmt"
	"github.com/shelmesky/gms/server/common"
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
	fmt.Printf("read from socket: %d bytes\n", n)
	b.WriteBytes(buffer[:n], n)

	return n
}

func (b *SocketBuffer) ReadBytes(buffer []byte, size int) int {
	originSize := size
	n := 0
	for {
		length := 0
		size = int(math.Min(float64(size), float64(b.in-b.out)))
		length = int(math.Min(float64(size), float64(b.size-(b.out&(b.size-1)))))
		start := b.out & (b.size - 1)
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
}

func (b *SocketBuffer) WriteBytes(buffer []byte, size int) int {
	fmt.Printf("write bytes: %d bytes\n", len(buffer))
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
	buffer := make([]byte, 4)
	length := b.ReadBytes(buffer, 4)
	if length != 4 {
		return 0, fmt.Errorf("read zero length")
	}

	return binary.LittleEndian.Uint32(buffer), nil
}

func (b *SocketBuffer) ReadUint64() (uint64, error) {
	buffer := make([]byte, 8)
	length := b.ReadBytes(buffer, 8)
	if length != 8 {
		return 0, fmt.Errorf("read zero length")
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
	client.ReadBuffer = NewSocketBuffer(4096, conn)
	client.WriteBuffer = NewSocketBuffer(4096, conn)
	return client
}

func HandleConnection(client Client) {
	for {
		totalLength, err := client.ReadBuffer.ReadUint64()
		if err != nil {
			fmt.Println("read failed:", err)
			break
		}

		buffer := make([]byte, totalLength)

		packetReadLen := client.ReadBuffer.ReadBytes(buffer, int(totalLength))
		if packetReadLen == 0 {
			fmt.Println("connection lost")
			break
		}

		requestStartPos := 0
		requestEndPos := common.REQUEST_LEN
		requestBytes := buffer[requestStartPos:requestEndPos]
		request := common.BytesToRequest(requestBytes, common.REQUEST_LEN)

		metaDataStartPos := requestEndPos
		metaDataEndPos := metaDataStartPos + int(request.MetaDataLength)
		metaData := buffer[metaDataStartPos:metaDataEndPos]

		bodyStartPos := metaDataEndPos
		bodyEndPos := bodyStartPos + int(request.BodyLength)
		body := buffer[bodyStartPos:bodyEndPos]

		fmt.Println("receive request:", request)
		fmt.Println("receive metadata",  string(metaData))
		fmt.Println("receive body", string(body))
	}

	fmt.Println("close connection:", client.Conn.Close())
}

func StartServer(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Accept() failed:", err)
			continue
		}

		tcpConn := conn.(*net.TCPConn)
		sockFile, err := tcpConn.File()
		if err != nil {
			panic(err.Error())
		}
		client := NewClient(tcpConn)
		client.SockFD = int(sockFile.Fd())

		go HandleConnection(client)
	}
}

func Run(address string) {
	listen, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	StartServer(listen)
}
