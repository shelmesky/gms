package main

import (
	"flag"
	"github.com/shelmesky/gms/server/common"
	"github.com/shelmesky/gms/server/server"
	log "github.com/sirupsen/logrus"
	"os"
	"os/signal"
	"syscall"
)

var (
	listenAddress    = flag.String("address", "0.0.0.0", "server listen address")
	listenPort       = flag.Int("port", 50051, "server listen port")
	rpcListenAddress = flag.String("rpc-address", "0.0.0.0", "rpc server listen address")
	rpcListenPort    = flag.Int("rpc-port", 50052, "rpc server listen port")
	nodeID           = flag.String("node-id", "node-0", "ID of current node")
	dataDir          = flag.String("data-dir", "./data", "local data dir")
	etcdServer       = flag.String("etcd-server", "127.0.0.1:2379", "etcd server")
	ipAddress        = flag.String("ipaddress", "127.0.0.1", "ip address for communication")
	inSyncReplicas   = flag.Int("insync-replicas", 2, "in sync replicas for partition SYNC")
)

func main() {
	flag.Parse()
	Init()

	customFormatter := new(log.TextFormatter)
	customFormatter.TimestampFormat = "2006-01-02 15:04:05.0000"
	customFormatter.FullTimestamp = true
	log.SetFormatter(customFormatter)
	log.SetLevel(log.TraceLevel)

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT)
	go func() {
		for {
			select {
			case <-c:
				log.Warningln("Server exit\n")
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
	common.GlobalConfig.InSyncReplicas = *inSyncReplicas
}
