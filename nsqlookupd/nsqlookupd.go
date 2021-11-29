package nsqlookupd

import (
	"github.com/xswwhy/nsq/internal/util"
	"github.com/xswwhy/nsq/internal/version"
	"log"
	"net"
	"os"
	"sync"
)

type NSQLookupd struct {
	sync.RWMutex
	opts         *Options
	tcpListener  net.Listener // 监听nsqd
	httpListener net.Listener // 监听nsqadmin
	tcpServer    *tcpServer
	watiGroup    util.WaitGroupWrapper
	DB           *RegistrationDB // 所有的nsqd都在这里面注册
}

func New(opts *Options) (*NSQLookupd, error) {
	//var err error
	if opts.Logger == nil { // 这里opts.Logger 有可能会触发空指针错误
		opts.Logger = log.New(os.Stderr, opts.LogPrefix, log.Ldate|log.Ltime|log.Lmicroseconds)
	}
	l := &NSQLookupd{
		opts: opts,
		DB:   NewRegistrationDB(),
	}
	l.logf(LOG_INFO, version.String("nsqlookup"))

	return l, nil
}
