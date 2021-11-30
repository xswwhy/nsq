package protocol

import (
	"fmt"
	"github.com/xswwhy/nsq/internal/lg"
	"net"
	"runtime"
	"strings"
	"sync"
)

type TCPHandler interface {
	Handle(conn net.Conn)
}

// TCPServer 作为TCP服务的启动入口,最后调用的TCPHandler接口
// 可想而知,nsqlookupd  nsqd 都需要实现自己的TCPHandler
// nsqlookupd的Handle处理nsqd的连接  //FIXME: nsqd的Handle处理客户端的连接
func TCPServer(listener net.Listener, handler TCPHandler, logf lg.AppLogFunc) error {
	logf(lg.INFO, "TCP: listening on %s", listener.Addr())

	var wg sync.WaitGroup
	for {
		clientConn, err := listener.Accept()
		if err != nil {
			// 如果是短暂的网络错误,runtime.Gosched()让出时间片,过一会再试一下
			if nerr, ok := err.(net.Error); ok && nerr.Temporary() {
				logf(lg.WARN, "temporary Accept() failure - %s", err)
				runtime.Gosched()
				continue
			}

			// FIXME: 感觉这里也是出于无奈才这么写的吧? 作者可能也不知道为什么会有 use of closed network connection
			if !strings.Contains(err.Error(), "use of closed network connection") {
				return fmt.Errorf("listener.Accept() error - %s", err)
			}
			break
		}
		wg.Add(1)
		go func() {
			handler.Handle(clientConn)
			wg.Done()
		}()
	}
	wg.Wait() // 就算TcpServer要退出了,也要等所有的Handle处理完
	logf(lg.INFO, "TCP: closing %s", listener.Addr())
	return nil
}
