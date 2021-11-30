package nsqlookupd

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/xswwhy/nsq/internal/protocol"
	"github.com/xswwhy/nsq/internal/version"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

type LookupProtocolV1 struct {
	nsqlookupd *NSQLookupd
}

func (p *LookupProtocolV1) NewClient(conn net.Conn) protocol.Client {
	return NewClientV1(conn)
}

// IOLoop 处理TCP连接的read write
func (p *LookupProtocolV1) IOLoop(c protocol.Client) error {
	var err error
	var line string

	client := c.(*ClientV1) // FIXME: 这里为什么是 *ClientV1 ,不是 ClinetV1
	reader := bufio.NewReader(client)
	for { // 开始IOLoop
		line, err = reader.ReadString('\n')
		if err != nil {
			break
		}
		line := strings.TrimSpace(line)
		params := strings.Split(line, " ")

		var response []byte
		response, err = p.Exec(client, reader, params)
		if err != nil {
			ctx := ""
			if parentErr := err.(protocol.ChildErr).Parent(); parentErr != nil {
				ctx = " - " + parentErr.Error()
			}
			// 带着父错误类型一起记录
			p.nsqlookupd.logf(LOG_ERROR, "[%s] - %s%s", client, err, ctx)

			// 将错误信息发送给client
			_, sendErr := protocol.SendResponse(client, []byte(err.Error()))
			if sendErr != nil {
				p.nsqlookupd.logf(LOG_ERROR, "[%s] - %s%s", client, sendErr, ctx)
				break
			}

			if _, ok := err.(*protocol.FatalClientErr); ok {
				break // 出现致命错误IOLoop就要退了,断开TCP连接
			}

			continue // 其他错误IOLoop要继续循环
		}

		if response != nil {
			_, err = protocol.SendResponse(client, response)
			if err != nil {
				break
			}
		}
	}

	// 走到这说明TCP连接出问题了 // FIXME:nsqd 主动断开了?
	p.nsqlookupd.logf(LOG_INFO, "PROTOCOL(V1): [%s] exiting ioloop", client)

	// nsqlookupd.DB 中删除该client
	if client.peerInfo != nil {
		registrations := p.nsqlookupd.DB.LookupRegistrations(client.peerInfo.id)
		for _, r := range registrations {
			if removed, _ := p.nsqlookupd.DB.RemoveProducer(r, client.peerInfo.id); removed {
				p.nsqlookupd.logf(LOG_INFO, "DB: client(%s) UNREGISTRATION category:%s key:%s subkey:%s",
					client, r.Category, r.Key, r.SubKey)
			}
		}
	}
	return err
}

// 处理client发来的数据
// 支持4种操作 PING  IDENTIFY  REGISTER  UNREGISTER
// 一个nsqd过来要先 IDENTIFY 再 REGISTER
func (p *LookupProtocolV1) Exec(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	switch params[0] {
	case "PING":
		return p.PING(client, params)
	case "IDENTIFY":
		return p.IDENTIFY(client, reader, params[1:])
	case "REGISTER":
		return p.REGISTER(client, reader, params[1:])
	case "UNREGISTER":
		return p.UNREGISTER(client, reader, params[1:])
	}
	return nil, protocol.NewFatalClientErr(nil, "E_INVALID", fmt.Sprintf("invalid command %s", params[0]))
}

func (p *LookupProtocolV1) PING(client *ClientV1, params []string) ([]byte, error) {
	if client.peerInfo != nil {
		cur := time.Unix(0, atomic.LoadInt64(&client.peerInfo.lastUpdate))
		now := time.Now()
		p.nsqlookupd.logf(LOG_INFO, "CLIENT(%s): pinged (last ping %s)", client.peerInfo.id,
			now.Sub(cur))
		atomic.StoreInt64(&client.peerInfo.lastUpdate, now.UnixNano())
	}
	return []byte("OK"), nil
}

// nsqlookupd处理nsqd发来的网络配置信息,并将自己的网洛配置信息发送给nsqd
func (p *LookupProtocolV1) IDENTIFY(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	var err error
	if client.peerInfo != nil { // 已经有peerInfo的肯定不对,刚建立连接的client是没有peerInfo的
		return nil, protocol.NewFatalClientErr(err, "E_INVALID", "cannot IDENTIFY again")
	}

	var bodyLen int32
	err = binary.Read(reader, binary.BigEndian, bodyLen)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to read body size")
	}
	body := make([]byte, bodyLen)
	_, err = io.ReadFull(reader, body)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to read body")
	}

	// 获取到了网络配置信息(json格式),存一下
	peerInfo := PeerInfo{id: client.RemoteAddr().String()}
	err = json.Unmarshal(body, &peerInfo)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to decode JSON body")

	}
	peerInfo.RemoteAddress = client.RemoteAddr().String()
	// peerInfo中的字段,一个都不能少
	if peerInfo.BroadcastAddress == "" || peerInfo.TCPPort == 0 || peerInfo.HTTPPort == 0 || peerInfo.Version == "" {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY", "IDENTIFY missing fields")
	}
	atomic.StoreInt64(&peerInfo.lastUpdate, time.Now().UnixNano())
	p.nsqlookupd.logf(LOG_INFO, "CLIENT(%s): IDENTIFY Address:%s TCP:%d HTTP:%d Version:%s",
		client, peerInfo.BroadcastAddress, peerInfo.TCPPort, peerInfo.HTTPPort, peerInfo.Version)

	client.peerInfo = &peerInfo
	if p.nsqlookupd.DB.AddProducer(Registration{"client", "", ""}, &Producer{peerInfo: client.peerInfo}) {
		p.nsqlookupd.logf(LOG_INFO, "DB: client(%s) REGISTER category:%s key:%s subkey:%s", client, "client", "", "")
	}

	// nsqlookupd给nsqd发送自己的网络配置信息
	data := make(map[string]interface{})
	data["tcp_port"] = p.nsqlookupd.RealTCPAddr().Port
	data["http_port"] = p.nsqlookupd.RealHTTPAddr().Port
	data["version"] = version.Binary
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("ERROR: unable to get hostname %s", err) // FIXME:奇怪?这里怎么突然不用p.nsqlookupd.logf()了
	}
	data["broadcast_address"] = p.nsqlookupd.opts.BroadcastAddress
	data["hostname"] = hostname
	response, err := json.Marshal(data)
	if err != nil {
		p.nsqlookupd.logf(LOG_ERROR, "json marshaling %v", data)
		return []byte("OK"), nil
	}
	return response, nil
}

func (p *LookupProtocolV1) REGISTER(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	if client.peerInfo == nil {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "client must IDENTIFY")
	}
	topic, channel, err := getTopicChan("REGISTER", params)
	if err != nil {
		return nil, err
	}

	// topic是一定有的,但是channel却可以没有
	// topic 对应NSQLookupd.DB中Registration中的Key
	// channel 对应NSQLookupd.DB中Registration中的SubKey
	// FIXME: 从代码上看,Registration中Category是固定写死的
	if channel != "" {
		key := Registration{"channel", topic, channel}
		if p.nsqlookupd.DB.AddProducer(key, &Producer{peerInfo: client.peerInfo}) {
			p.nsqlookupd.logf(LOG_INFO, "DB: client(%s) REGISTER category:%s key:%s subkey:%s",
				client, "channel", topic, channel)
		}
	}
	key := Registration{"topic", topic, ""}
	if p.nsqlookupd.DB.AddProducer(key, &Producer{peerInfo: client.peerInfo}) {
		p.nsqlookupd.logf(LOG_INFO, "DB: client(%s) REGISTER category:%s key:%s subkey:%s",
			client, "topic", topic, "")
	}
	return []byte("OK"), nil
}

func (p *LookupProtocolV1) UNREGISTER(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	if client.peerInfo == nil {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "client must IDENTIFY")
	}
	topic, channel, err := getTopicChan("UNREGISTER", params)
	if err != nil {
		return nil, err
	}
	if channel != "" {
		k := Registration{"channel", topic, channel}
		removed, left := p.nsqlookupd.DB.RemoveProducer(k, client.peerInfo.id)
		if removed {
			p.nsqlookupd.logf(LOG_INFO, "DB: client(%s) UNREGISTER category:%s key:%s subkey:%s",
				client, "channel", topic, channel)
		}
		// 有#ephemeral 标记的topic 或者 channel, Registration为空的时候,连Registration也删
		if left == 0 && strings.HasSuffix(channel, "#ephemeral") {
			p.nsqlookupd.DB.RemoveRegistration(k)
		}
	} else {
		// 没有channel的时候,表示同一topic下的所有channel都删
		registrations := p.nsqlookupd.DB.FindRegistrations("channel", topic, "*")
		for _, registration := range registrations {
			removed, _ := p.nsqlookupd.DB.RemoveProducer(registration, client.peerInfo.id)
			if removed {
				p.nsqlookupd.logf(LOG_INFO, "DB: client(%s) UNREGISTER category:%s key:%s subkey:%s",
					client, "topic", topic, "")
			}
		}
		key := Registration{"topic", topic, ""}
		removed, left := p.nsqlookupd.DB.RemoveProducer(key, client.peerInfo.id)
		if removed {
			p.nsqlookupd.logf(LOG_INFO, "DB: client(%s) UNREGISTER category:%s key:%s subkey:%s",
				client, "topic", topic, "")
		}
		if left == 0 && strings.HasSuffix(topic, "#ephemeral") {
			p.nsqlookupd.DB.RemoveRegistration(key)
		}
	}
	return []byte("OK"), nil
}

// 从params中获取 topic 和 channel
// command参数都是写死的,只是为了日志中方便排查问题
func getTopicChan(command string, params []string) (string, string, error) {
	if len(params) == 0 {
		return "", "", protocol.NewFatalClientErr(nil, "E_INVALID", fmt.Sprintf("%s insufficient number of params", command))
	}
	topicName := params[0]
	var channelName string
	if len(params) >= 2 {
		channelName = params[1]
	}
	if !protocol.IsValidTopicName(topicName) {
		return "", "", protocol.NewFatalClientErr(nil, "E_BAD_TOPIC", fmt.Sprintf("%s topic name '%s' is not valid", command, topicName))
	}
	if channelName != "" && !protocol.IsValidChannelName(channelName) {
		return "", "", protocol.NewFatalClientErr(nil, "E_BAD_CHANNEL", fmt.Sprintf("%s channel name '%s' is not valid", command, channelName))
	}
	return topicName, channelName, nil
}
