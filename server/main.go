package main

import (
	"flag"
	"fmt"
	"github.com/shelmesky/gms/server/common"
	"github.com/shelmesky/gms/server/server"
	"os"
	"os/signal"
	"syscall"
)

var (
	listenAddress = flag.String("address", "127.0.0.1", "listen address")
	listenPort    = flag.Int("port", 50051, "listen port")
	nodeID        = flag.String("node-id", "node-1", "ID of current node")
	dataDir       = flag.String("data-dir", "./data", "local data dir")
	etcdServer    = flag.String("etcd-server", "127.0.0.1:2379", "etcd server")
)

func main() {
	flag.Parse()
	Init()

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT)
	go func() {
		for {
			select {
			case <-c:
				fmt.Printf("Server exit\n")
				os.Exit(1)
			}
		}
	}()

	server.Run(*listenAddress, *listenPort)
}

func Init() {
	common.GlobalConfig.ListenAddress = *listenAddress
	common.GlobalConfig.ListenPort = *listenPort
	common.GlobalConfig.NodeID = *nodeID
	common.GlobalConfig.DataDir = *dataDir
	common.GlobalConfig.EtcdServer = *etcdServer
}
