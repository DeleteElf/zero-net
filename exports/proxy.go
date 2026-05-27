package exports

/*
   #cgo CFLAGS: -I ../output

   #include <string.h>
   #include "network-quic.h"
*/
import "C"
import (
	"fmt"
	"github.com/DeleteElf/network-quic/agent"
	"github.com/DeleteElf/network-quic/framework/streams"
	"github.com/DeleteElf/network-quic/framework/utils"
	"log/slog"
	"time"
)

//export ProxyServerCreate
func ProxyServerCreate(config *C.NetworkData) C.int {
	if config == nil {
		return C.ErrorParam
	}
	jsonObject, err := utils.GetJsonObject(FromBytes(config))
	if err != nil {
		return C.ErrorParam
	}
	data := jsonObject["data"].(map[string]interface{})
	if data == nil {
		return C.ErrorParam
	}
	url := fmt.Sprintf("%s/device?type=proxy&apikey=%s",
		jsonObject["mgr_addr"].(string), jsonObject["apikey"].(string))
	cfg := &agent.Config{
		MgrAddr:  url,
		Hearts:   50,
		Data:     data,
		Version:  "1",
		SignSalt: "2fbbdf99eae1675484a48e8310db1ee42d3bd6fdbc5e3f3755af848b23cc9817",
	}
	if managerCtx != nil && !managerCtx.IsClosed {
		return C.Success
	}
	managerCtx = agent.NewManagePlatform(cfg)
	if managerCtx == nil {
		return C.ErrorContext
	}
	go func() {
		for {
			if managerCtx == nil || managerCtx.IsClosed { //如果服务已经关闭，则不再继续连接管理平台
				break
			}
			managerCtx.ConnectToPlatform()
			go managerCtx.Hearts() //维持心跳
			err1 := managerCtx.ListenAgentConnect(func(sock *streams.Socket) {
				socketMap[sock.Id] = sock
				slog.Debug("新的客户端接入：", slog.String("id", sock.Id))
				if onAcceptSocket != nil {
					C.callMessageCallback(onAcceptSocket, C.CString(sock.Id))
				}
			}, func(sock *streams.Socket) {
				if onDisConnected != nil {
					C.callMessageCallback(onDisConnected, C.CString(sock.Id))
				}
				delete(socketMap, sock.Id)
			})
			if err1 != nil {
				slog.Debug("监听管理平台的websocket发生错误，5秒后重试！", slog.Any("err", err))
				time.Sleep(5 * time.Second)
				continue
			}
			if !managerCtx.IsClosed { //如果服务已经关闭，则不再继续连接管理平台
				slog.Debug("与管理平台断开连接，5秒后重试！")
				time.Sleep(5 * time.Second)
			}
		}
	}()
	return C.Success
}

//export ProxyServerSocketClose
func ProxyServerSocketClose(clientId *C.char) C.int {
	if managerCtx == nil {
		slog.Warn("请先创建服务端代理实例！")
		return C.ErrorContext
	}
	cliId := C.GoString(clientId)
	if len(cliId) == 0 {
		return C.ErrorParam
	}
	svr := managerCtx.GetServer(cliId)
	if svr == nil {
		return C.ErrorParam
	}
	err := svr.CloseSocket(cliId)
	if err != nil {
		slog.Warn("关闭socket失败！")
		return C.ErrorClose
	}
	slog.Debug("socket执行关闭逻辑完成")
	return C.Success
}
