package main

/*
#cgo CFLAGS: -I ../output

#include <string.h>
//在Go1.6.2版本之后，Go的runtime 加入了指针违规传递检测机制。该机制主要针对Go向C传递带有指向其他Go内存的地址，具体见文献检查机制。
//Go 1.25.9 支持自动将此处代码生成.h文件，因此，我们不再引用.h文件，而是将必须的内容书写在此

enum LogLevel {
    LevelFatal = 0,
    LevelError,
    LevelWarn,
    LevelInfo,
    LevelDebug,
    // LevelTrace,
    LevelMax
};
//声明日志回调
typedef void (*LogCallback)(const char*);
typedef void (*AcceptSocket)(const char*);
typedef struct _NetworkData {
    int len;
    char *ptr;
} NetworkData;

typedef struct _ClientData {
	char* id;
	int index;
    int len;
    char* ptr;
} ClientData;

enum NetworkResult {
    Success = 0,
    Error=80000,
    ErrorContext,
    ErrorParam,
    ErrorSocket,
    ErrorBuffer,
    ErrorClose,
    Closed
};

static void call_logCallback(const char* msg,LogCallback callback){
	if(callback){
		callback(msg);
	}
}

static void call_onAcceptSocket(const char* id,AcceptSocket callback){
	if(callback){
		callback(id);
	}
}

*/
import "C"
import (
	"github.com/DeleteElf/network-quic/client"
	utils2 "github.com/DeleteElf/network-quic/framework/utils"
	"github.com/DeleteElf/network-quic/server"
	"github.com/DeleteElf/network-quic/streams"
	"log/slog"
	"reflect"
	"unsafe"
)

type AcceptInfo struct {
	ChannelCount  int    `json:"count,default=1"`
	ClientId      string `json:"id" validate:"required"`
	ServerAddress string `json:"server" validate:"required"`
}

func FromBytes(data *C.NetworkData) []byte {
	if data.ptr != nil && data.len > 0 {
		return (*[1 << 30]byte)(unsafe.Pointer(data.ptr))[:data.len:data.len]
	}
	return []byte{}
}

func CopyBytes(src []byte, data *C.NetworkData) C.int {
	srcLen := len(src)
	if srcLen > int(data.len) { //如果来源数据比缓冲区大，则报错？不是说可以根据缓冲区大小读取数据吗？
		data.len = C.int(srcLen)
		return C.ErrorBuffer
	}
	C.memcpy(unsafe.Pointer(data.ptr), unsafe.Pointer(unsafe.SliceData(src)), C.size_t(srcLen))
	data.len = C.int(srcLen)
	return C.Success
}

func CopyStr(src string, data *C.NetworkData) C.int {
	srcLen := len(src)
	if srcLen > int(data.len) {
		data.len = C.int(srcLen)
		return C.ErrorBuffer
	}
	C.memcpy(unsafe.Pointer(data.ptr), unsafe.Pointer(unsafe.StringData(src)), C.size_t(srcLen))
	data.len = C.int(srcLen)
	return C.Success
}

var serverCtx *server.Server
var clientCtx *client.Client

var g_log_level int = -1

//export InitLog
func InitLog(level C.int) {
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
	utils2.InitLog(slogLevel, nil)
}

var logCallback C.LogCallback

type logCallbackWriter struct{}

func (logCallbackWriter) Write(p []byte) (n int, err error) {
	C.call_logCallback(C.CString(string(p)), logCallback)
	//if logCallback != nil {
	//	logCallback(C.CString(string(p)))
	//}
	return len(p), nil
}

//export InitLogCallback
func InitLogCallback(level C.int, callback C.LogCallback) {
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
	utils2.InitLog(slogLevel, logCallbackWriter{})
}

//export ClientCreate
func ClientCreate() C.int {
	slog.Info("log", slog.Int("level", g_log_level))
	if g_log_level < 0 {
		utils2.InitLog(slog.LevelDebug, nil)
	}
	InitProcess()
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
	jsonObject, err := utils2.GetJsonObject(FromBytes(config))
	if err != nil {
		return C.ErrorParam
	}
	address := jsonObject["address"].(string)
	id := jsonObject["id"].(string)
	networkType := jsonObject["networkType"].(string)
	if networkType != streams.STREAM_NETWORK_UDP {
		return C.ErrorParam
	}
	clientCtx = client.NewClient(address, id)                              //尝试连接本机服务
	err = clientCtx.Connect(int(channelCount), streams.STREAM_NETWORK_UDP) //创建udp网络
	if err != nil {
		slog.Error("客户端连接失败", slog.Any("err", err))
		return C.ErrorClose
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
	slog.Info("log", slog.Int("level", g_log_level))
	if g_log_level < 0 {
		utils2.InitLog(slog.LevelDebug, nil)
	}
	InitProcess()

	jsonObject, err := utils2.GetJsonObject(FromBytes(config))
	if err != nil {
		return C.ErrorParam
	}
	address := jsonObject["address"].(string)
	networkType := jsonObject["networkType"].(string)
	if networkType != streams.STREAM_NETWORK_UDP {
		return C.ErrorParam
	}
	serverCtx = server.NewServer(address, false) //尝试连接本机服务
	return C.Success
}

//export ServerClose
func ServerClose() C.int {
	if serverCtx == nil {
		slog.Warn("未检索到有效的服务端！")
		return C.ErrorContext
	}
	serverCtx = nil
	return C.Success
}

var onAcceptSocket C.AcceptSocket

//export ServerSetOnAcceptSocket
func ServerSetOnAcceptSocket(callback C.AcceptSocket) C.int {
	if onAcceptSocket != nil && callback != nil {
		return C.ErrorParam
	}
	if serverCtx == nil {
		slog.Warn("请先创建服务端实例！")
		return C.ErrorContext
	}
	onAcceptSocket = callback
	if onAcceptSocket == nil {
		return C.Success
	}
	go func() {
		for {
			if onAcceptSocket == nil {
				break
			}
			select {
			case id := <-serverCtx.OnAccept:
				//if onAcceptSocket != nil {
				//	onAcceptSocket(C.CString(id))
				//}
				C.call_onAcceptSocket(C.CString(id), onAcceptSocket)
			}
		}
	}()
	return C.Success
}

//export ServerStartListen
func ServerStartListen() C.int {
	if serverCtx == nil {
		slog.Warn("请先创建服务端实例！")
		return C.ErrorContext
	}
	go serverCtx.StartListen()
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
	slog.Debug("执行关闭逻辑完成")
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
	if serverCtx == nil {
		slog.Warn("请先创建服务端实例！")
		return C.ErrorContext
	}
	if len(serverCtx.Sockets) == 0 {
		return C.Success
	}
	if currentBuffer == nil {
		slog.Debug("将数据从通道读入缓存！")
		cases := make([]reflect.SelectCase, len(serverCtx.Sockets)*3)
		index := 0
		for _, sock := range serverCtx.Sockets { //将所有通道加入到列表
			for i := 0; i < sock.ChannelCount; i++ {
				cases[index] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(sock.StreamChannels[i].Channel)}
				index++
			}
		}
		_, value, ok := reflect.Select(cases) //执行监听所有通道
		if !ok {
			return C.ErrorClose
		}
		buffer := value.Interface().(streams.StreamChannelData)
		currentBuffer = &buffer
	}
	slog.Debug("正在读取缓存数据！")
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

func main() {
}
