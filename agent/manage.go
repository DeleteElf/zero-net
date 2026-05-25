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
	"net"
	"time"

	"github.com/gorilla/websocket"
)

type Config struct {
	MgrAddr string           `json:"mgr_addr"`
	Hearts  int              `json:"hearts"`
	Data    utils.JsonObject `json:"data"`
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
	//平台对接的代理
	Agents []*Agent
	//平台对接的服务,一个平台只能一个服务,一个服务可以对接多个代理
	Server *server.Server
}

func NewManagePlatform(cfg *Config) *ManagePlatform {
	websocket.DefaultDialer.HandshakeTimeout = 10 * time.Second
	websocket.DefaultDialer.TLSClientConfig = &tls.Config{
		InsecureSkipVerify: true,
	}
	mgr := &ManagePlatform{
		AgentStreamChannel: make(chan AgentStream), //创建
		Agents:             make([]*Agent, 0),      //创建代理空间
	}
	mgr.IsClosed = false
	go func() {
		for {
			if mgr.IsClosed { //如果服务已经关闭，则不再继续连接管理平台
				break
			}
			if err := mgr.connectToAgent(cfg); err != nil {
				slog.Debug("未与管理平台连接成功，5秒后重试！", slog.Any("err", err))
				time.Sleep(5 * time.Second)
			}
			if !mgr.IsClosed { //如果服务已经关闭，则不再继续连接管理平台
				slog.Debug("与管理平台断开连接，5秒后重试！")
				time.Sleep(5 * time.Second)
			}
		}
		slog.Debug("与管理平台结束连接！")
	}()
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

func (mgr *ManagePlatform) connectToAgent(cfg *Config) error {
	ws, _, err := websocket.DefaultDialer.Dial(cfg.MgrAddr, nil)
	if err != nil {
		return err
	}
	mgr.wsConn = ws
	defer mgr.Close()
	data := PlatformActionInfo{
		Action: ACTION_REG,
		From:   "host",
		Info:   cfg.Data,
	}
	if err = mgr.sendJson(data); err != nil {
		return err
	}
	tsMsg := time.Now()
	go func() {
		tickerDuration := time.Duration(cfg.Hearts) * time.Second
		expireDuration := time.Duration(cfg.Hearts+10) * time.Second
		ticker := time.NewTicker(tickerDuration)
		defer ticker.Stop()
		for range ticker.C {
			if mgr.wsConn == nil {
				break
			}
			if mgr.IsClosed {
				break
			}
			if tsMsg.Add(expireDuration).Compare(time.Now()) < 0 {
				slog.Warn("platform pong time out")
				_ = ws.Close()
				break
			}
			_ = mgr.sendJson(PlatformActionInfo{
				Action: "ping",
				From:   "host",
			})
		}
	}()

	for {
		if mgr.wsConn == nil {
			break
		}
		if mgr.IsClosed {
			break
		}
		_, msg, err := ws.ReadMessage()
		if err != nil {
			slog.Error("mgr return message error:", slog.Any("err", err))
			break
		}
		slog.Debug("mgr return message:", slog.Any("msg", msg))
		tsMsg = time.Now() //任何消息都更新时间

		var proxy ActionProxy
		if err := json.Unmarshal(msg, &proxy); err != nil {
			slog.Warn("json Unmarshal err", slog.Any("err", err))
			continue
		}
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
			slog.Debug("从管理平台获取的代理地址：", slog.String("address", proxyInfo.ProxyAddr))
			if _, err := net.ResolveUDPAddr("udp", proxyInfo.ProxyAddr); err != nil {
				slog.Info("mgr resp proxy invalid", slog.String("addr", proxyInfo.ProxyAddr), slog.Any("err", err))
				continue
			}
			//if mgr.server == nil { //如果是与管理平台临时断开连接，我们这里不进行重连
			slog.Debug("正在连接代理的预设地址...", slog.String("addr", proxyInfo.ProxyAddr), slog.Int("idx", proxyInfo.Idx))
			agent, err := NewAgent(proxyInfo.ProxyAddr, uint32(proxyInfo.Idx), 1)
			if err != nil {
				slog.Error("连接代理失败", slog.Any("err", err))
				if agent.NetConn != nil {
					_ = agent.NetConn.Close()
				}
				continue
			}
			agent.Proxy = &proxyInfo
			mgr.Agents = append(mgr.Agents, agent) //连接成功即可
			if mgr.Server == nil {
				mgr.Server = server.NewServer(agent.NetConn, true)
			}
			slog.Debug("代理服务创建成功！", slog.Int("idx", proxyInfo.Idx))
			break
		}
		//if mgr.Agent != nil {
		//	slog.Debug("代理服务创建成功！", slog.Int("idx", proxyInfo.Idx))
		//} else {
		//	slog.Debug("代理服务创建失败！", slog.Int("idx", proxyInfo.Idx))
		//}
	}
	return nil
}
