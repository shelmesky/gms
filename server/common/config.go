package common

var (
	GlobalConfig *Config
)

const (
	VERSION = 1000
)

func init() {
	GlobalConfig = new(Config)
}

// 全局配置
type Config struct {
	ListenAddress    string // 监听地址
	ListenPort       int    // 监听端口
	RPCListenAddress string // RPC服务监听地址
	RPCListenPort    int    // RPC服务监听端口
	NodeID           string // 节点的名称
	DataDir          string // 数据保存的目录
	EtcdServer       string // etcd服务器地址
	IPAddress        string // Node之间通信的IP地址
}
