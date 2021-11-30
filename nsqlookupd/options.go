package nsqlookupd

import "github.com/xswwhy/nsq/internal/lg"

type Options struct {
	// 日志相关
	LogLever  lg.LogLevel
	LogPrefix string
	Logger    Logger

	// 服务相关
	TCPAddress       string
	BroadcastAddress string `flag:"broadcast-address"`
}
