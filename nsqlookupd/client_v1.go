package nsqlookupd

import (
	"net"
)

type ClientV1 struct {
	net.Conn
	peerInfo *PeerInfo
}

func NewClientV1(conn net.Conn) *ClientV1 {
	// 注意:刚建立连接的Client是没有peerInfo的
	return &ClientV1{Conn: conn}
}
