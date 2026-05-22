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
	//Ts   int64  `json:"ts"`   // 时间戳
	//Sign string `json:"sign"` // 鉴权，用于校验本次请求是否合法，**以防动态库被不可信第三方调用**
	//EndPoint string      `json:"end_point"`
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
}

func NewManagePlatform(cfg *Config) *ManagePlatform {
	websocket.DefaultDialer.HandshakeTimeout = 10 * time.Second
	websocket.DefaultDialer.TLSClientConfig = &tls.Config{
		InsecureSkipVerify: true,
	}
	mgr := &ManagePlatform{
		AgentStreamChannel: make(chan AgentStream), //创建
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
		if len(proxy.Data.ProxyAddr) == 0 {
			proxy.Data.ProxyAddr = proxy.Data.ProxyExternalIp + ":" + proxy.Data.ProxyExternalPort
		}
		slog.Debug("尝试连接的代理地址：", slog.String("address", proxy.Data.ProxyAddr))
		if _, err := net.ResolveUDPAddr("udp", proxy.Data.ProxyAddr); err != nil {
			slog.Info("mgr resp proxy invalid", slog.String("addr", proxy.Data.ProxyAddr), slog.Any("err", err))
			continue
		}
		//if mgr.server == nil { //如果是与管理平台临时断开连接，我们这里不进行重连
		slog.Debug("accept new proxy host connecting", slog.String("addr", proxy.Data.ProxyAddr), slog.Int("idx", proxy.Data.Idx))

		netConn, _, err := NewAgent(proxy.Data.ProxyAddr, uint32(proxy.Data.Idx), 1)
		if err != nil {
			slog.Error("new server sock fail", slog.Any("err", err))
			if netConn != nil {
				netConn.Close()
			}
			return nil
		}
		//startServer(true, &mgr.AgentStreamChannel, netConn)
		slog.Debug("代理服务创建成功！")
		//}
	}
	//mgr.server.Close()
	return nil
}
