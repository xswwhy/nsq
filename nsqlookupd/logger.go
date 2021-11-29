package nsqlookupd

import "github.com/xswwhy/nsq/internal/lg"

type Logger lg.Logger

const (
	LOG_DEBUG = lg.DEBUG
	LOG_INFO  = lg.DEBUG
	LOG_WARN  = lg.WARN
	LOG_ERROR = lg.ERROR
	LOG_FATAL = lg.FATAL
)

func (n *NSQLookupd) logf(level lg.LogLevel, f string, args ...interface{}) {
	lg.Logf(n.opts.Logger, n.opts.LogLever, level, f, args)
}
