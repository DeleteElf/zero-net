package agent

import (
	"crypto/tls"
	"encoding/json"
	"github.com/DeleteElf/network-quic/framework"
	"github.com/DeleteElf/network-quic/framework/streams"
	"github.com/DeleteElf/network-quic/framework/utils"
	"github.com/DeleteElf/network-quic/server"
	"github.com/deleteelf/goframework/utils/jsonhelper"
	"github.com/quic-go/quic-go"
	"log/slog"
	"time"

	"github.com/gorilla/websocket"
)

type AgentMessageCallbackFunc func(int, string)

type Config struct {
	MgrAddr string           `json:"mgr_addr"`
	Hearts  int              `json:"hearts"`
	Data    utils.JsonObject `json:"data"`
	//Port    int              `json:"port"` //因为代理中心的问题，暂时无法直接设置端口
}

type AgentStream struct {
	Info   *streams.StreamInfo
	Stream *quic.Stream
	Server *server.Server
}

type AcceptClientConnect func(chan AgentStream)

type ManagePlatform struct {
	//用于串联代理中心和主机之间的通讯
	AgentStreamChannel chan AgentStream
	//与管理平台之间的连接
	wsConn *websocket.Conn
	framework.CloseableObject
	Agents map[int]*Agent
	config *Config

	//Server          *server.Server //目前代理中心，只能在未建立quic通讯前打通路由，因此不能在此建立服务对象
	lastMessageTime time.Time
}

func NewManagePlatform(cfg *Config) *ManagePlatform {
	websocket.DefaultDialer.HandshakeTimeout = 10 * time.Second
	websocket.DefaultDialer.TLSClientConfig = &tls.Config{
		InsecureSkipVerify: true,
	}
	ws, _, err := websocket.DefaultDialer.Dial(cfg.MgrAddr, nil)
	if err != nil {
		return nil
	}
	data := PlatformActionInfo{
		Action: ACTION_REG,
		From:   "host",
		Info:   cfg.Data,
	}
	mgr := &ManagePlatform{
		AgentStreamChannel: make(chan AgentStream), //创建
		Agents:             make(map[int]*Agent),   //创建代理空间
		wsConn:             ws,
		config:             cfg,
		lastMessageTime:    time.Now(),
	}
	mgr.IsClosed = false
	if err = mgr.sendJson(data); err != nil {
		return nil
	}
	go mgr.Hearts() //心跳
	//todo: 测试直接建立服务端，与代理中心无法通讯
	//mgr.Server = server.NewServerByAddress("0.0.0.0:" + strconv.Itoa(cfg.Port)) //直接建立服务端
	//todo: 测试直接建立udp客户端，固定ip时，无法与代理中心建立通讯
	//listenAddr, err := net.ResolveUDPAddr(streams.STREAM_NETWORK_UDP, "0.0.0.0:"+strconv.Itoa(cfg.Port))
	//if err != nil {
	//	return nil
	//}
	//conn, err := net.ListenUDP(streams.STREAM_NETWORK_UDP, listenAddr)
	//if err != nil {
	//	return nil
	//}
	//mgr.Server = server.NewServer(conn, true)
	//
	//mgr.Server.OnAcceptSocket = func(id string) {
	//	slog.Debug("test accept socket", slog.String("id", id))
	//}
	//go mgr.Server.StartListen(func(id string) {
	//	slog.Debug("test disconnect socket", slog.String("id", id))
	//})
	return mgr
}

func (mgr *ManagePlatform) OnClosing() {
	if mgr.wsConn != nil {
		_ = mgr.wsConn.Close()
		mgr.wsConn = nil
	}
}

func (mgr *ManagePlatform) OnClosed() {
	slog.Debug("管理平台已经断开！")
}

func (mgr *ManagePlatform) sendJson(v any) error {
	jsonString, err := jsonhelper.ToJsonString(v)
	if err != nil {
		return err
	}
	return mgr.send(jsonString)
}

func (mgr *ManagePlatform) send(msg string) error {
	if mgr.wsConn != nil {
		slog.Info("发送消息：", slog.Any("msg", msg))
		return mgr.wsConn.WriteMessage(websocket.TextMessage, []byte(msg))
	}
	return nil
}

func (mgr *ManagePlatform) Hearts() {
	tickerDuration := time.Duration(mgr.config.Hearts) * time.Second
	expireDuration := time.Duration(mgr.config.Hearts+10) * time.Second
	ticker := time.NewTicker(tickerDuration)
	defer ticker.Stop()
	for range ticker.C {
		if mgr.wsConn == nil {
			break
		}
		if mgr.IsClosed {
			break
		}
		if mgr.lastMessageTime.Add(expireDuration).Compare(time.Now()) < 0 {
			slog.Warn("platform pong time out")
			_ = mgr.wsConn.Close()
			break
		}
		_ = mgr.sendJson(PlatformActionInfo{
			Action: "ping",
			From:   "host",
		})
	}
}

func (mgr *ManagePlatform) ListenAgentConnect(onAcceptSocket, onDisconnect AgentMessageCallbackFunc) error {
	for {
		if mgr.wsConn == nil {
			break
		}
		if mgr.IsClosed {
			break
		}
		_, msg, err := mgr.wsConn.ReadMessage()
		if err != nil {
			slog.Error("mgr return message error:", slog.Any("err", err))
			break
		}
		slog.Debug("mgr return message:", slog.Any("msg", msg))
		var proxy ActionProxy
		if err := json.Unmarshal(msg, &proxy); err != nil {
			slog.Warn("json Unmarshal err", slog.Any("err", err))
			continue
		}
		mgr.lastMessageTime = time.Now()
		if !proxy.IsSuccess() {
			slog.Warn("action not success")
			continue
		}
		if proxy.Action != "proxy" {
			//slog.Debug("mgr resp", slog.String("action", proxy.Action))
			continue
		}
		proxyInfo := proxy.Data
		if len(proxyInfo.ProxyAddr) == 0 {
			proxyInfo.ProxyAddr = proxyInfo.ProxyIp + ":" + proxyInfo.ProxyPort
		}
		count := 1
		if proxyInfo.AllowExternal {
			count = 2
		}
		for i := 0; i < count; i++ {
			if len(proxyInfo.ProxyAddr) == 0 { //平台没有返回有效地址
				continue
			}
			if i == 1 {
				proxyInfo.ProxyAddr = proxyInfo.ProxyExternalIp + ":" + proxyInfo.ProxyExternalPort
			}
			//slog.Debug("从管理平台获取的代理地址：", slog.String("address", proxyInfo.ProxyAddr))
			//proxyAddr, err := net.ResolveUDPAddr("udp", proxyInfo.ProxyAddr)
			//if err != nil { //检查代理地址是否有效
			//	slog.Info("mgr resp proxy invalid", slog.String("addr", proxyInfo.ProxyAddr), slog.Any("err", err))
			//	continue
			//}
			if mgr.Agents[proxyInfo.Idx] == nil { //如果这个代理还没有连接，则进行连接
				slog.Debug("正在连接代理授权地址...", slog.String("addr", proxyInfo.ProxyAddr), slog.Int("idx", proxyInfo.Idx))
				conn, proxyAddr, err := streams.NewUdpSocketClient(proxyInfo.ProxyAddr)
				if err != nil {
					slog.Error("连接代理服务器失败", slog.Any("err", err))
					continue
				}
				//agent, err := NewAgentService(mgr.Server.NetConn, proxyAddr, uint32(proxyInfo.Idx), 1)
				agent, err := NewAgentService(conn, proxyAddr, uint32(proxyInfo.Idx), 1)
				if err != nil {
					slog.Error("连接代理失败", slog.Any("err", err))
					//if agent.NetConn != nil {
					//	_ = agent.NetConn.Close()
					//}
					continue
				}
				agent.Proxy = &proxyInfo
				mgr.Agents[proxyInfo.Idx] = agent //连接成功即可
				agent.Server = server.NewServer(agent.Socket, true)
				agent.Server.OnAcceptSocket = func(id string) {
					onAcceptSocket(agent.Proxy.Idx, id)
				}
				go agent.Server.StartListen(func(id string) {
					onDisconnect(agent.Proxy.Idx, id)
				})
				slog.Debug("代理服务创建成功！", slog.Int("idx", proxyInfo.Idx))
				//} else {
				//	_ = mgr.Agents[proxyInfo.Idx].AddAuthAgent(uint32(proxyInfo.Idx), 1)
				//	slog.Debug("代理服务已存在，继续使用！", slog.Int("idx", proxyInfo.Idx))
			}
			break
		}
	}
	return nil
}
