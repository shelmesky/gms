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
	listenAddress    = flag.String("address", "0.0.0.0", "server listen address")
	listenPort       = flag.Int("port", 50051, "server listen port")
	rpcListenAddress = flag.String("rpc-address", "0.0.0.0", "rpc server listen address")
	rpcListenPort    = flag.Int("rpc-port", 50052, "rpc server listen port")
	nodeID           = flag.String("node-id", "node-1", "ID of current node")
	dataDir          = flag.String("data-dir", "./data", "local data dir")
	etcdServer       = flag.String("etcd-server", "127.0.0.1:2379", "etcd server")
	ipAddress        = flag.String("ipaddress", "127.0.0.1", "ip address for communication")
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

	go server.RunRPC(*rpcListenAddress, *rpcListenPort)
	server.Run(*listenAddress, *listenPort)
}

func Init() {
	common.GlobalConfig.ListenAddress = *listenAddress
	common.GlobalConfig.ListenPort = *listenPort
	common.GlobalConfig.RPCListenAddress = *rpcListenAddress
	common.GlobalConfig.RPCListenPort = *rpcListenPort
	common.GlobalConfig.NodeID = *nodeID
	common.GlobalConfig.DataDir = *dataDir
	common.GlobalConfig.EtcdServer = *etcdServer
	common.GlobalConfig.IPAddress = *ipAddress
}
