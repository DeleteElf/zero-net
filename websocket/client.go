package websocket

import (
	"crypto/tls"
	"github.com/DeleteElf/network-quic/framework"
	"github.com/deleteelf/goframework/utils/jsonhelper"
	"github.com/gorilla/websocket"
	"log/slog"
	"time"
)

type Client struct {
	conn            *websocket.Conn
	heartTicker     *time.Ticker
	lastMessageTime time.Time
	lastHeartTime   time.Time
	HeartTimeout    time.Duration
	framework.CloseableObject
	OnMessage      func(msg jsonhelper.JsonObject)
	OnConnected    func()
	OnDisconnected func()
}

func NewClient() *Client {
	return &Client{
		HeartTimeout: time.Second * 50,
	}
}

func (c *Client) Connect(address, heartMessage string) error {
	websocket.DefaultDialer.HandshakeTimeout = 10 * time.Second
	websocket.DefaultDialer.TLSClientConfig = &tls.Config{
		InsecureSkipVerify: true,
	}
	ws, _, err := websocket.DefaultDialer.Dial(address, nil)
	if err != nil {
		return err
	}
	c.conn = ws
	if c.OnConnected != nil {
		c.OnConnected()
	}
	go func() {
		for {
			if c.conn == nil {
				break
			}
			if c.IsClosed {
				break
			}
			_, msg, err := c.conn.ReadMessage()
			if err != nil {
				return
			}
			c.lastMessageTime = time.Now()
			slog.Debug("receive message:", slog.Any("msg", msg))
			result, err := jsonhelper.GetJsonObject(msg)
			if err != nil {
				return
			}
			if result["Action"] == "pong" {
				continue
			}
			if c.OnMessage != nil {
				c.OnMessage(result)
			}
		}
	}()
	if len(heartMessage) == 0 {
		heartMessage = "{\"Action\":\"ping\",\"From\":\"host\"}"
	}
	c.Heart(heartMessage)
	return nil
}

func (c *Client) Heart(heartMessage string) {
	tickerDuration := c.HeartTimeout * time.Second
	expireDuration := (c.HeartTimeout + 10) * time.Second
	c.heartTicker = time.NewTicker(time.Second) //每秒检查一次
	defer func() {
		if c.heartTicker != nil {
			c.heartTicker.Stop()
			c.heartTicker = nil
		}
	}()
	for range c.heartTicker.C {
		if c.conn == nil {
			break
		}
		if c.IsClosed {
			break
		}
		if c.lastMessageTime.Add(expireDuration).Compare(time.Now()) < 0 {
			slog.Warn("pong time out")
			c.Close()
			break
		}
		if c.lastHeartTime.Add(tickerDuration).Compare(time.Now()) < 0 {
			c.lastHeartTime = time.Now()
			_ = c.Send(heartMessage)
		}
	}
	slog.Warn("stop ping heart")
}

func (c *Client) OnClosing() bool {
	slog.Debug("正在断开websocket连接...")
	if c.OnDisconnected != nil {
		c.OnDisconnected()
	}
	if c.heartTicker != nil {
		c.heartTicker.Stop()
		c.heartTicker = nil
	}
	return true
}

func (c *Client) OnClosed() {
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
	slog.Debug("websocket已经断开！")
}

func (c *Client) SendJson(v any) error {
	jsonString, err := jsonhelper.ToJsonString(v)
	if err != nil {
		return err
	}
	return c.Send(jsonString)
}

func (c *Client) Send(msg string) error {
	if c.conn != nil && !c.IsClosed {
		slog.Debug("发送消息：", slog.Any("msg", msg))
		return c.conn.WriteMessage(websocket.TextMessage, []byte(msg))
	}
	return nil
}
