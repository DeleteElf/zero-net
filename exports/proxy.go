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
	managerCtx = agent.NewManagePlatform(cfg)
	if managerCtx == nil {
		return C.ErrorContext
	}
	return C.Success
}

//export ProxyServerClose
func ProxyServerClose() C.int {
	onAcceptSocket = nil
	if managerCtx != nil {
		managerCtx.Close()
		managerCtx = nil
	}
	return C.Success
}

//export ProxyServerStartListen
func ProxyServerStartListen() C.int {
	if managerCtx == nil {
		slog.Warn("请先创建服务端代理实例！")
		return C.ErrorContext
	}
	go func() {
		for {
			if managerCtx.IsClosed { //如果服务已经关闭，则不再继续连接管理平台
				break
			}
			if err := managerCtx.ListenAgentConnect(func(sock *streams.Socket) {
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
			}); err != nil {
				slog.Debug("未与管理平台连接成功，5秒后重试！", slog.Any("err", err))
				time.Sleep(5 * time.Second)
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

//export ProxyServerSocketSend
func ProxyServerSocketSend(clientId *C.char, chnIdx C.int, data *C.NetworkData) C.int {
	if data == nil {
		return C.ErrorParam
	}
	if managerCtx == nil {
		slog.Warn("请先创建服务端代理实例！")
		return C.ErrorContext
	}
	cliId := C.GoString(clientId)
	if len(cliId) == 0 {
		return C.ErrorParam
	}
	sock := managerCtx.GetServerSocket(cliId)
	if sock == nil {
		return C.ErrorSocket
	}
	success, err := sock.Send(int(chnIdx), FromBytes(data))
	if err != nil {
		slog.Error("写入流发生错误", slog.Any("err", err))
		svr := managerCtx.GetServer(cliId)
		_ = svr.CloseSocket(cliId)
		return C.ErrorClose
	}
	if success {
		return C.Success
	}
	return C.Closed
}

//
//var proxyCurrentBuffer *streams.StreamChannelData
//
////export ProxyServerSocketReceive
//func ProxyServerSocketReceive(data *C.ClientData) C.int {
//	if data == nil {
//		return C.ErrorParam
//	}
//	if mgr == nil {
//		slog.Warn("请先创建服务端代理实例！")
//		return C.ErrorContext
//	}
//	for {
//		if mgr.IsClosed { //如果等待的过程，结束了，则退出
//			return C.ErrorContext
//		}
//		if len(mgr.Agents) == 0 { //等待接入
//			time.Sleep(time.Millisecond)
//			continue
//		}
//		break //正式工作
//	}
//	if proxyCurrentBuffer == nil {
//		cases := make([]reflect.SelectCase, len(mgr.Agents)*3)
//		index := 0
//		//slog.Debug("尝试获取缓存", slog.Int("socket数量", len(serverCtx.Sockets)))
//		for _, agt := range mgr.Agents { //将所有通道加入到列表
//			for _, sock := range agt.Server.Sockets {
//				for i := 0; i < sock.ChannelCount; i++ {
//					cases[index] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(sock.StreamChannels[i].Channel)}
//					index++
//				}
//			}
//		}
//		_, value, ok := reflect.Select(cases) //执行监听所有通道
//		if !ok {
//			return C.ErrorClose
//		}
//		buffer := value.Interface().(streams.StreamChannelData)
//		//slog.Debug("获取到缓存", slog.String("clientId", buffer.ClientId), slog.Int("channelId", buffer.ChannelId))
//		proxyCurrentBuffer = &buffer
//	}
//	if len(proxyCurrentBuffer.Data) == 0 {
//		slog.Debug("获取到缓存大小异常")
//		return C.ErrorBuffer
//	}
//	data.index = C.int(proxyCurrentBuffer.ChannelId)
//	data.id = C.CString(proxyCurrentBuffer.ClientId)
//
//	bufferSize := len(proxyCurrentBuffer.Data)
//	bufferMaxSize := int(data.len)
//	if bufferMaxSize == -1 { //为支持零拷贝，这里提供外部提供-1缓冲区长度的支持
//		bufferMaxSize = bufferSize
//		copySize := min(bufferSize-proxyCurrentBuffer.Offset, bufferMaxSize) //考虑到外部输入可能书写不严谨，零拷贝支持提供剩余的缓存
//		data.ptr = (*C.char)(unsafe.Pointer(uintptr(unsafe.Pointer(&proxyCurrentBuffer.Data[0])) + uintptr(proxyCurrentBuffer.Offset)))
//		data.len = C.int(copySize)
//	} else {
//		copySize := min(bufferSize-proxyCurrentBuffer.Offset, bufferMaxSize) //修改成根据缓冲区大小来读取数据
//		C.memcpy(unsafe.Pointer(data.ptr), unsafe.Pointer(uintptr(unsafe.Pointer(&proxyCurrentBuffer.Data[0]))+uintptr(proxyCurrentBuffer.Offset)), C.size_t(copySize))
//		data.len = C.int(copySize)
//	}
//	proxyCurrentBuffer.Offset += int(data.len)
//	if proxyCurrentBuffer.Offset >= bufferSize {
//		proxyCurrentBuffer = nil
//	}
//	return C.Success
//}
