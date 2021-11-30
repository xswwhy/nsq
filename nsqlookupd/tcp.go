package nsqlookupd

import (
	"github.com/xswwhy/nsq/internal/protocol"
	"io"
	"net"
	"sync"
)

type tcpServer struct {
	nsqlookupd *NSQLookupd // 这个传过来好像没什么用,只是为了使用他的logf()函数
	conns      sync.Map    // 存储所有的TCP连接
}

// 处理单个tcp连接,这个函数会被多个goroutine同时调用
func (p *tcpServer) Handle(conn net.Conn) {
	p.nsqlookupd.logf(LOG_INFO, "TCP: new clinet(%s", conn.RemoteAddr())

	// 先读一个版本号
	buf := make([]byte, 4)
	_, err := io.ReadFull(conn, buf)
	if err != nil {
		p.nsqlookupd.logf(LOG_ERROR, "failed to read protocal version - %s", err)
		conn.Close()
		return
	}
	protocalMagic := string(buf)
	p.nsqlookupd.logf(LOG_INFO, "CLIENT(%s): desired protocol magic '%s'",
		conn.RemoteAddr(), protocalMagic)

	var prot protocol.Protocol
	switch protocalMagic { // 确定protocol版本号
	case "  V1":
		prot = &LookupProtocolV1{nsqlookupd: p.nsqlookupd}
	default:
		protocol.SendResponse(conn, []byte("E_BAD_PROTOCOL"))
		conn.Close()
		p.nsqlookupd.logf(LOG_WARN, "client(%s) bad protocol magic '%s'", conn.RemoteAddr(), protocalMagic)
		return
	}
	client := prot.NewClient(conn)
	p.conns.Store(conn.RemoteAddr(), client)
	// IOLoop 处理TCP连接的read write
	err = prot.IOLoop(client)
	if err != nil {
		// 这里用了LOG_ERROR 而不是LOG_INFO或者LOG_WARN
		// 是因为nsqd是长期运行的,一旦连上nsqlookupd就不会轻易断开,除非出现了错误
		p.nsqlookupd.logf(LOG_ERROR, "client(%s) - %s", conn.RemoteAddr(), err)
	}
	p.conns.Delete(conn.RemoteAddr())
	client.Close()
}

// 将所有Client的TCP连接断掉
func (p *tcpServer) Close() {
	p.conns.Range(func(k, v interface{}) bool {
		v.(protocol.Client).Close()
		return true
	})
}
