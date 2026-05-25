package tests

import (
	"github.com/DeleteElf/network-quic/agent"
	"github.com/DeleteElf/network-quic/client"
	"github.com/DeleteElf/network-quic/framework/utils"
	"github.com/DeleteElf/network-quic/server"
	"log/slog"
	"testing"
	"time"
)

func agentSocketHandler(svr *server.Server, clientId string) {
	sock := svr.GetSocket(clientId)
	if sock == nil {
		slog.Error("客户端已经不存在！")
		return
	}
	for i := 0; i < sock.ChannelCount; i++ {
		go messageHandler(svr, clientId, sock, i)
	}
}

func TestHostAgent(t *testing.T) {
	utils.InitLog(slog.LevelDebug, nil)
	cfg := &agent.Config{
		MgrAddr: "wss://192.168.199.159:3005/device?type=proxy&apikey=575D6618206A2754",
		Hearts:  50,
		Data: utils.JsonObject(map[string]interface{}{
			"device_id": "0A76DE8C-1AB1-35C3-A137-FC9E10B1EF9F",
		}),
	}
	mgr := agent.NewManagePlatform(cfg)
	for {
		if mgr.Server == nil { //等待创建
			time.Sleep(10 * time.Millisecond)
			continue
		}
		mgr.Server.OnAcceptSocket = func(id string) {
			slog.Debug("新的客户端接入：", slog.String("id", id))
			go agentSocketHandler(mgr.Server, id)
		}
		mgr.Server.StartListen(func(id string) {
			slog.Debug("客户端断开连接：", slog.String("id", id))
		})
		slog.Debug("服务端退出监听！")
		mgr.Server.Close()
		mgr.Server = nil
		if !restart {
			break
		}
	}
}

func TestClientAgent(t *testing.T) {
	utils.InitLog(slog.LevelDebug, nil)
	//{Ts:1779441784
	//Sign:
	//MgrAddr:https://192.168.199.159:3005
	//Token:0DBDB1AE-CABD-F2BA-4F89-132A39EC90D1
	//DevId:0A76DE8C-1AB1-35C3-A137-FC9E10B1EF9F
	//ProxyId:A7C569D2-F0A7-7B0C-BB2A-E44D59D5A5EB
	//SvrAddr:192.168.199.22
	//ProxyInfo:true
	//CliId:1a1f2dadcd90473bb684bcd02b9cc629
	//NetType:udp
	//SvcType:udp
	//SvcAddr:192.168.199.22
	//LoAddr: UpBuf:0
	//DownBuf:0}
	request := &agent.Requst{
		MgrAddr: "https://192.168.199.159:3005",
		Token:   "0DBDB1AE-CABD-F2BA-4F89-132A39EC90D1",
		DevId:   "0A76DE8C-1AB1-35C3-A137-FC9E10B1EF9F",
		ProxyId: "A7C569D2-F0A7-7B0C-BB2A-E44D59D5A5EB",
		SvrAddr: "192.168.199.22",
		Proxy:   true,
		CliId:   "1a1f2dadcd90473bb684bcd02b9cc629",
		NetType: "udp",
	}
	proxy, err := agent.GetProxy(request)
	if err != nil {
		return
	}
	proxy.ProxyAddr = proxy.ProxyIp + ":" + proxy.ProxyPort
	cli := client.NewClient(proxy.ProxyAddr, request.CliId) //尝试连接本机服务
	agt, err := agent.NewAgent(cli.ServerAddress, uint32(proxy.Idx), 0)
	if err == nil && agt != nil {
		cli.ConnectToAgent(3, agt.NetConn, agt.RemoteAddress, func(id string) {
			slog.Debug("socket已经断开===》！")
		}) //创建udp网络
	}
	if cli.Socket == nil {
		slog.Info("客户端连接失败！")
		return
	}

	for i := 0; i < cli.Socket.ChannelCount; i++ {
		go receiveHandler(cli, i)
	}
	msg0 := "hello,i am channel 0 data from client"
	slog.Info("正在向通道0发送数据", slog.String("msg", msg0))
	_, _ = cli.Socket.Send(0, []byte(msg0))
	msg1 := "hello,i am channel 1 data from client"
	slog.Info("正在向通道1发送数据", slog.String("msg", msg1))
	_, _ = cli.Socket.Send(1, []byte(msg1))

	msg2 := "hello,i am channel 2 data from client"
	slog.Info("正在向通道2发送数据", slog.String("msg", msg2))
	_, _ = cli.Socket.Send(2, []byte(msg2))

	time.Sleep(time.Second * 10)
}
