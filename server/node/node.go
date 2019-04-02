package node

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/shelmesky/gms/server/common"
	"go.etcd.io/etcd/clientv3"
	"log"
	"time"
)

// 保存在etcd中的Node信息
type Node struct {
	IPAddress string `json:"ip_address"`
	Port      int    `json:"port"`
	NodeID    string `json:"node_id"`
	StartTime int64  `json:"start_time"`
}

func Start() (chan struct{}, error) {
	var err error
	var stopChan chan struct{}
	var client *clientv3.Client

	client, err = clientv3.New(clientv3.Config{
		Endpoints: []string{common.GlobalConfig.EtcdServer},
	})

	if err != nil {
		return stopChan, errors.Wrap(err, "connect to kv server failed")
	}

	kv := clientv3.NewKV(client)

	lease := clientv3.NewLease(client)

	leaseResp, err := lease.Grant(context.TODO(), 5)
	if err != nil {
		return stopChan, errors.Wrap(err, "lease grant failed")
	}

	key := fmt.Sprintf("/brokers/ids/%s", common.GlobalConfig.NodeID)
	value := getNodeInfo()
	putResp, err := kv.Put(context.TODO(), key, value, clientv3.WithLease(leaseResp.ID))
	if err != nil {
		return stopChan, errors.Wrap(err, "kv put failed")
	}
	log.Println("set brokers info", putResp)

	stopChan = make(chan struct{}, 1)

	go func() {
		select {
		case <-stopChan:
			return
		default:
			for {
				keepResp, err := lease.KeepAliveOnce(context.TODO(), leaseResp.ID)
				if err != nil {
					log.Fatal(errors.Wrap(err, "KeepAliveOnce failed"))
				}
				log.Println("KeepAliveOnce:", keepResp.String())

				time.Sleep(time.Second * 2)
			}
		}
	}()

	return stopChan, nil
}

func getNodeInfo() string {
	var nodeInfo Node
	nodeInfo.IPAddress = common.GlobalConfig.IPAddress
	nodeInfo.Port = common.GlobalConfig.ListenPort
	nodeInfo.NodeID = common.GlobalConfig.NodeID
	nodeInfo.StartTime = time.Now().Unix()

	jsonBytes, err := json.Marshal(nodeInfo)
	if err != nil {
		return ""
	}

	return string(jsonBytes)
}
