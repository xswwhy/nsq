package protocol

import (
	"encoding/binary"
	"io"
	"net"
)

type Client interface {
	Close() error
}

// 网络协议接口
type Protocol interface {
	NewClient(conn net.Conn) Client
	IOLoop(Client) error
}

func SendResponse(w io.Writer, data []byte) (int, error) {
	// 使用binary.Write而不是w.Write()是因为网络上的字节流要大端的,而内存里面的基本上都是小端存储的
	// int32(len(data))  golang中int对应的是int64,转成int32肯定够用,可以省四个字节
	err := binary.Write(w, binary.BigEndian, int32(len(data)))
	if err != nil {
		return 0, err
	}
	n, err := w.Write(data)
	if err != nil {
		return 0, err
	}
	// n+4 这个4对应上面int32(len(data)), 不然就要n+8
	return (n + 4), nil
}

func SendFramedResponse(w io.Writer, frameType int32, data []byte) (int, error) {
	buBuf := make([]byte, 4)

	size := uint32(len(data) + 4) // 这里的 +4 加的是下面frameType 占的4个字节,size本身的4个字节是不算里面的
	binary.BigEndian.PutUint32(buBuf, size)
	//先发长度
	n, err := w.Write(buBuf)
	if err != nil {
		return n, err
	}

	binary.BigEndian.PutUint32(buBuf, uint32(frameType))
	// 再发类型
	n, err = w.Write(buBuf)
	if err != nil {
		return n + 4, err
	}

	// 最后发数据类型
	n, err = w.Write(data)
	if err != nil {
		return n + 8, err
	}
	return n + 8, nil
}
