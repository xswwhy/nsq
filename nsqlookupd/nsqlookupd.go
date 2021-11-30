package nsqlookupd

import (
	"fmt"
	"github.com/xswwhy/nsq/internal/protocol"
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
	var err error
	if opts.Logger == nil { // 这里opts.Logger 有可能会触发空指针错误
		opts.Logger = log.New(os.Stderr, opts.LogPrefix, log.Ldate|log.Ltime|log.Lmicroseconds)
	}
	l := &NSQLookupd{
		opts: opts,
		DB:   NewRegistrationDB(),
	}
	l.logf(LOG_INFO, version.String("nsqlookup"))

	l.tcpServer = &tcpServer{nsqlookupd: l}
	l.tcpListener, err = net.Listen("tcp", opts.TCPAddress)
	if err != nil {
		return nil, fmt.Errorf("listrn (%s) failed - %s", opts.TCPAddress, err)
	}

	return l, nil
}

func (l *NSQLookupd) Main() error {
	exitChain := make(chan error)
	var once sync.Once
	exifFunc := func(err error) {
		once.Do(func() {
			if err != nil {
				l.logf(LOG_FATAL, "%s", err)
			}
			exitChain <- err
		})
	}
	l.watiGroup.Wrap(func() { // FIXME: 这里不是很明白为什么要用waitGroup包一下,直接go启动不行嘛?
		exifFunc(protocol.TCPServer(l.tcpListener, l.tcpServer, l.logf))
	})

	err := <-exitChain
	return err
}

func (l *NSQLookupd) RealTCPAddr() *net.TCPAddr {
	return l.tcpListener.Addr().(*net.TCPAddr)
}

func (l *NSQLookupd) RealHTTPAddr() *net.TCPAddr {
	return l.httpListener.Addr().(*net.TCPAddr)
}

func (l *NSQLookupd) Exit() {
	if l.tcpListener != nil {
		l.tcpListener.Close()
	}
	if l.tcpServer != nil {
		l.tcpServer.Close()
	}
	if l.httpListener != nil {
		l.httpListener.Close()
	}
	l.watiGroup.Wait()
}
