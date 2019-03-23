package common

var (
	GlobalConfig *Config
)

func init() {
	GlobalConfig = new(Config)
}

type Config struct {
	ListenAddress string
	ListenPort    int
	NodeID        string
	DataDir       string
	EtcdServer    string
}
