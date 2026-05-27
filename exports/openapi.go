package exports

/*
#cgo CFLAGS: -I ../output

#include <string.h>
#include "network-quic.h"
*/
import "C"
import (
	"github.com/DeleteElf/network-quic/agent"
	"github.com/DeleteElf/network-quic/client"
	"github.com/DeleteElf/network-quic/framework/streams"
	"github.com/DeleteElf/network-quic/framework/utils"
	"github.com/DeleteElf/network-quic/server"
	"log/slog"
	"reflect"
	"time"
	"unsafe"
)

func FromBytes(data *C.NetworkData) []byte {
	if data.ptr != nil && data.len > 0 {
		return (*[1 << 30]byte)(unsafe.Pointer(data.ptr))[:data.len:data.len]
	}
	return []byte{}
}

var serverCtx *server.Server
var clientCtx *client.Client
var managerCtx *agent.ManagePlatform
var socketMap map[string]*streams.Socket

//var channelCaseList []reflect.SelectCase //这个如果没有每次重新构建，似乎有问题

var g_log_level int = -1

type logCallbackWriter struct{}

func (logCallbackWriter) Write(p []byte) (n int, err error) {
	C.callMessageCallback(logCallback, C.CString(string(p)))
	return len(p), nil
}

var logCallback C.MessageCallback

//export InitLogCallback
func InitLogCallback(level C.int, callback C.MessageCallback) {
	g_log_level = int(level)
	slogLevel := slog.LevelInfo
	switch level {
	case C.LevelFatal:
		slogLevel = slog.LevelError
	case C.LevelError:
		slogLevel = slog.LevelError
	case C.LevelWarn:
		slogLevel = slog.LevelWarn
	case C.LevelInfo:
		slogLevel = slog.LevelInfo
	case C.LevelDebug:
		slogLevel = slog.LevelDebug
	}
	logCallback = callback
	utils.InitLog(slogLevel, logCallbackWriter{})
}

//export InitNetwork
func InitNetwork() C.int {
	slog.Info("log", slog.Int("level", g_log_level))
	if g_log_level < 0 {
		utils.InitLog(slog.LevelDebug, nil)
	}
	utils.InitProcess()
	socketMap = make(map[string]*streams.Socket) //初始化全局链路缓存
	return C.Success
}

var onAcceptSocket C.MessageCallback

//export SetOnAcceptSocketCallback
func SetOnAcceptSocketCallback(callback C.MessageCallback) C.int {
	if onAcceptSocket != nil && callback != nil {
		return C.ErrorParam
	}
	if serverCtx == nil {
		slog.Warn("请先创建服务端实例！")
		return C.ErrorContext
	}
	onAcceptSocket = callback
	return C.Success
}

var onDisConnected C.MessageCallback

//export SetOnDisConnectedCallback
func SetOnDisConnectedCallback(callback C.MessageCallback) C.int {
	if onDisConnected != nil && callback != nil {
		return C.ErrorParam
	}
	if serverCtx == nil {
		slog.Warn("请先创建服务端实例！")
		return C.ErrorContext
	}
	onDisConnected = callback
	return C.Success
}

//export ClientClose
func ClientClose() C.int {
	if clientCtx == nil {
		slog.Warn("未检索到有效的客户端！")
		return C.ErrorContext
	}
	clientCtx.Close()
	clientCtx = nil
	return C.Success
}

//export ClientConnect
func ClientConnect(channelCount C.int, config *C.NetworkData) C.int {
	if config == nil {
		return C.ErrorParam
	}
	jsonObject, err := utils.GetJsonObject(FromBytes(config))
	if err != nil {
		return C.ErrorParam
	}
	address := jsonObject["address"].(string)
	id := jsonObject["id"].(string)
	networkType := jsonObject["networkType"].(string)
	if networkType != streams.STREAM_NETWORK_UDP {
		return C.ErrorParam
	}
	if jsonObject["proxy_id"] != nil { //如果配置了代理，则使用代理
		request := &agent.Requst{
			Proxy:   true,
			NetType: networkType,
			CliId:   id,
		}
		request.ProxyId = jsonObject["proxy_id"].(string)
		request.Token = jsonObject["token"].(string)
		request.DevId = jsonObject["dev_id"].(string)
		request.MgrAddr = jsonObject["mgr_addr"].(string)

		proxy, err := agent.GetProxy(request)
		if err != nil {
			return C.ErrorParam
		}
		proxy.ProxyAddr = proxy.ProxyExternalIp + ":" + proxy.ProxyExternalPort //使用外网地址连接
		clientCtx = client.NewClient(proxy.ProxyAddr, request.CliId)            //尝试连接本机服务

		cfg := &agent.Config{
			Version:  "1",
			SignSalt: "2fbbdf99eae1675484a48e8310db1ee42d3bd6fdbc5e3f3755af848b23cc9817",
		}
		agt, err := agent.NewAgent(clientCtx.ServerAddress, uint32(proxy.Idx), 0, cfg)
		if err == nil && agt != nil {
			sock := agt.Socket
			clientCtx.ConnectToAgent(3, sock, agt.RemoteAddress, func(sock *streams.Socket) {
				if onDisConnected != nil {
					C.callMessageCallback(onDisConnected, C.CString(sock.Id))
				}
			}) //创建udp网络
		}
	} else {
		clientCtx = client.NewClient(address, id) //尝试连接本机服务
		err = clientCtx.Connect(int(channelCount), streams.STREAM_NETWORK_UDP, func(sock *streams.Socket) {
			if onDisConnected != nil {
				C.callMessageCallback(onDisConnected, C.CString(sock.Id))
			}
		}) //创建udp网络
		if err != nil {
			slog.Error("客户端连接失败", slog.Any("err", err))
			return C.ErrorClose
		}
	}
	slog.Info("客户端连接成功！", slog.Int("通道数", clientCtx.Socket.ChannelCount))
	return C.Success
}

//export ClientChannelReceive
func ClientChannelReceive(chnIdx C.int, data *C.NetworkData) C.int {
	//基于通道的读取方式，严格按外部提供的缓存大小来操作
	if data == nil {
		return C.ErrorParam
	}
	if clientCtx == nil {
		slog.Warn("请先连接服务端！")
		return C.ErrorContext
	}
	if clientCtx.IsClosed {
		slog.Warn("请先连接服务端！")
		return C.Closed
	}

	channelId := int(chnIdx)
	_, err := clientCtx.Socket.ReceiveDataToBuffer(channelId) //这个会卡住等待
	if err != nil {
		slog.Warn(err.Error())
		return C.ErrorClose
	}
	buffer := clientCtx.Socket.StreamChannels[channelId].Buffer
	if buffer == nil {
		return C.ErrorBuffer
	}
	//这一段的逻辑 也可以使用bufio.Reader来实现，如果是纯go，更佳，但我们需要转C，自己实现的逻辑性能更佳
	bufferSize := len(buffer.Data)
	bufferMaxSize := int(data.len)
	copySize := min(bufferSize-buffer.Offset, bufferMaxSize) //修改成根据缓冲区大小来读取数据
	if copySize > 0 {
		C.memcpy(unsafe.Pointer(data.ptr), unsafe.Pointer(&buffer.Data[buffer.Offset]), C.size_t(copySize))
		data.len = C.int(copySize)
		buffer.Offset += copySize
	}
	if buffer.Offset >= bufferSize && channelId < clientCtx.Socket.ChannelCount {
		clientCtx.Socket.StreamChannels[channelId].Buffer = nil
	}
	return C.Success
}

//export ClientChannelSend
func ClientChannelSend(chnIdx C.int, data *C.NetworkData) C.int {
	if data == nil {
		return C.ErrorParam
	}
	if clientCtx == nil {
		slog.Warn("请先连接服务端！")
		return C.ErrorContext
	}
	if clientCtx.IsClosed {
		slog.Warn("请先连接服务端！")
		return C.Closed
	}
	success, err := clientCtx.Socket.Send(int(chnIdx), FromBytes(data))
	if err != nil {
		slog.Error("客户端发送数据发生错误", slog.Any("err", err))
		return C.Error
	}
	if success {
		return C.Success
	}
	return C.Closed
}

//export ServerCreate
func ServerCreate(config *C.NetworkData) C.int {
	if config == nil {
		return C.ErrorParam
	}

	jsonObject, err := utils.GetJsonObject(FromBytes(config))
	if err != nil {
		return C.ErrorParam
	}
	address := jsonObject["address"].(string)
	networkType := jsonObject["networkType"].(string)
	if networkType != streams.STREAM_NETWORK_UDP {
		return C.ErrorParam
	}
	serverCtx = server.NewServerByAddress(address) //尝试连接本机服务
	serverCtx.OnAcceptSocket = func(sock *streams.Socket) {
		socketMap[sock.Id] = sock
		if onAcceptSocket != nil {
			C.callMessageCallback(onAcceptSocket, C.CString(sock.Id))
		}
	}
	serverCtx.OnSocketDisConnected = func(sock *streams.Socket) {
		if onDisConnected != nil {
			C.callMessageCallback(onDisConnected, C.CString(sock.Id))
		}
		delete(socketMap, sock.Id)
	}
	return C.Success
}

//export ServerClose
func ServerClose() C.int {
	onAcceptSocket = nil
	if serverCtx != nil {
		serverCtx.Close()
		serverCtx = nil
	}
	return C.Success
}

//export ServerStartListen
func ServerStartListen() C.int {
	if serverCtx == nil {
		slog.Warn("请先创建服务端实例！")
		return C.ErrorContext
	}
	go serverCtx.StartListen(func(sock *streams.Socket) {
		if onDisConnected != nil {
			C.callMessageCallback(onDisConnected, C.CString(sock.Id))
		}
	})
	return C.Success
}

//export ServerSocketClose
func ServerSocketClose(clientId *C.char) C.int {
	if serverCtx == nil {
		slog.Warn("请先创建服务端实例！")
		return C.ErrorContext
	}
	cliId := C.GoString(clientId)
	if len(cliId) == 0 {
		return C.ErrorParam
	}
	err := serverCtx.CloseSocket(cliId)
	if err != nil {
		slog.Warn("关闭socket失败！")
		return C.ErrorClose
	}
	slog.Debug("socket执行关闭逻辑完成")
	return C.Success
}

//export ServerSocketSend
func ServerSocketSend(clientId *C.char, chnIdx C.int, data *C.NetworkData) C.int {
	if data == nil {
		return C.ErrorParam
	}
	if serverCtx == nil {
		slog.Warn("请先创建服务端实例！")
		return C.ErrorContext
	}
	cliId := C.GoString(clientId)
	if len(cliId) == 0 {
		return C.ErrorParam
	}
	sock := serverCtx.GetSocket(cliId)
	if sock == nil {
		return C.ErrorSocket
	}
	success, err := sock.Send(int(chnIdx), FromBytes(data))
	if err != nil {
		slog.Error("写入流发生错误", slog.Any("err", err))
		_ = serverCtx.CloseSocket(cliId)
		return C.ErrorClose
	}
	if success {
		return C.Success
	}
	return C.Closed
}

var currentBuffer *streams.StreamChannelData

//export ServerSocketReceive
func ServerSocketReceive(data *C.ClientData) C.int {
	if data == nil {
		return C.ErrorParam
	}
	if serverCtx == nil && managerCtx == nil {
		slog.Warn("请先创建服务端实例！")
		return C.ErrorContext
	}
	for {
		if serverCtx != nil && serverCtx.IsClosed { //如果等待的过程，结束了，则退出
			return C.ErrorContext
		}
		if len(socketMap) == 0 { //如果还没有接入，则执行等待
			time.Sleep(time.Millisecond)
			continue
		}
		break //正式工作
	}
	if currentBuffer == nil {
		count := len(socketMap) * 3
		channelCaseList := make([]reflect.SelectCase, count)
		index := 0
		for _, sock := range socketMap {
			for i := 0; i < sock.ChannelCount; i++ {
				channelCaseList[index] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(sock.StreamChannels[i].Channel)}
				index++
			}
		}
		_, value, ok := reflect.Select(channelCaseList) //执行监听所有通道
		if !ok {
			return C.ErrorClose
		}
		buffer := value.Interface().(streams.StreamChannelData)
		currentBuffer = &buffer
	}
	if len(currentBuffer.Data) == 0 {
		slog.Debug("获取到缓存大小异常")
		return C.ErrorBuffer
	}
	data.index = C.int(currentBuffer.ChannelId)
	data.id = C.CString(currentBuffer.ClientId)

	bufferSize := len(currentBuffer.Data)
	bufferMaxSize := int(data.len)
	if bufferMaxSize == -1 { //为支持零拷贝，这里提供外部提供-1缓冲区长度的支持
		bufferMaxSize = bufferSize
		copySize := min(bufferSize-currentBuffer.Offset, bufferMaxSize) //考虑到外部输入可能书写不严谨，零拷贝支持提供剩余的缓存
		data.ptr = (*C.char)(unsafe.Pointer(uintptr(unsafe.Pointer(&currentBuffer.Data[0])) + uintptr(currentBuffer.Offset)))
		data.len = C.int(copySize)
	} else {
		copySize := min(bufferSize-currentBuffer.Offset, bufferMaxSize) //修改成根据缓冲区大小来读取数据
		C.memcpy(unsafe.Pointer(data.ptr), unsafe.Pointer(uintptr(unsafe.Pointer(&currentBuffer.Data[0]))+uintptr(currentBuffer.Offset)), C.size_t(copySize))
		data.len = C.int(copySize)
	}
	currentBuffer.Offset += int(data.len)
	if currentBuffer.Offset >= bufferSize {
		currentBuffer = nil
	}
	return C.Success
}

//export ServerSocketChannelReceive
func ServerSocketChannelReceive(clientId *C.char, chnIdx C.int, data *C.NetworkData) C.int {
	if data == nil {
		return C.ErrorParam
	}
	if serverCtx == nil {
		slog.Warn("请先创建服务端实例！")
		return C.ErrorContext
	}
	cliId := C.GoString(clientId)
	if len(cliId) == 0 {
		return C.ErrorParam
	}
	sock := serverCtx.GetSocket(cliId)
	if sock == nil {
		return C.ErrorSocket
	}
	channelIndex := int(chnIdx)
	_, err := sock.ReceiveDataToBuffer(channelIndex) //这个会卡住等待
	if err != nil {
		slog.Warn(err.Error())
		return C.ErrorClose
	}
	if sock.IsClosed { //优化如果过程中断开后继续
		return C.Closed
	}
	if channelIndex >= sock.ChannelCount { //到这边说明是已经关闭了
		return C.Closed
	}
	buffer := sock.StreamChannels[channelIndex].Buffer
	//*chnIdx = C.int(currentBuffer.ChannelId)
	bufferSize := len(buffer.Data)
	bufferMaxSize := int(data.len)
	if bufferMaxSize == -1 { //为支持零拷贝，这里提供外部提供-1缓冲区长度的支持
		bufferMaxSize = bufferSize
		copySize := min(bufferSize-buffer.Offset, bufferMaxSize) //考虑到外部输入可能书写不严谨，零拷贝支持提供剩余的缓存
		data.ptr = (*C.char)(unsafe.Pointer(uintptr(unsafe.Pointer(&buffer.Data[0])) + uintptr(buffer.Offset)))
		data.len = C.int(copySize)
	} else {
		copySize := min(bufferSize-buffer.Offset, bufferMaxSize) //修改成根据缓冲区大小来读取数据
		C.memcpy(unsafe.Pointer(data.ptr), unsafe.Pointer(uintptr(unsafe.Pointer(&buffer.Data[0]))+uintptr(buffer.Offset)), C.size_t(copySize))
		data.len = C.int(copySize)
	}
	buffer.Offset += int(data.len)
	if buffer.Offset >= bufferSize {
		sock.StreamChannels[channelIndex].Buffer = nil
	}
	return C.Success
}
