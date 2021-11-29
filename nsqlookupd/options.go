package nsqlookupd

import "github.com/xswwhy/nsq/internal/lg"

type Options struct {
	LogLever  lg.LogLevel
	LogPrefix string
	Logger    Logger
}
