package main

import (
	"fmt"
	"google.golang.org/grpc"
	pb "github.com/shelmesky/gms/server/protobuf"
	"log"
	"time"
	"context"
)

const (
	address     = "127.0.0.1:50051"
	defaultName = "world"
)

func main() {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	c := pb.NewGMSClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	var message pb.MessageType
	message.CRC32 = 1234
	message.Magic = 999
	message.Attributes = 888
	message.KeyLength = 3
	message.KeyPayload = []byte("abc")
	message.ValueLength = 7
	message.ValuePayload = []byte("1234567")

	var writeRequest pb.WriteMessageRequest
	writeRequest.TopicName = "mytopic"
	writeRequest.Partition = "0"
	writeRequest.Message = &message

	response, err := c.SendMessage(ctx, &writeRequest)
	fmt.Println(response, err)
}
