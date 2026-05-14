package tests

import (
	"fmt"
	"github.com/DeleteElf/network-quic/client"
	"github.com/DeleteElf/network-quic/streams"
	"github.com/DeleteElf/network-quic/utils"
	"log/slog"
	"testing"
)

func receive(cli *client.Client, channelIndex int) {
	slog.Info("正在准备接收数据", slog.Int("channel", channelIndex))
	err := cli.ReceiveDataToBuffer(channelIndex)
	if !err {
		slog.Error("ReceiveDataToBuffer error", slog.Any("err", err))
		return
	}
	buffer := cli.CurrentBuffers[channelIndex]
	slog.Info("收到来自服务端的新消息", slog.Int("channel", channelIndex), slog.String("msg", string(buffer.Data)))
	cli.CurrentBuffers[channelIndex] = nil

	_, _ = cli.Send(cli.Streams[channelIndex], []byte("bye"))
}

func TestClient(t *testing.T) {
	utils.InitLog(slog.LevelDebug, nil)                  //初始化日志
	cli := client.NewClient("127.0.0.1:10001", "test01") //尝试连接本机服务

	err := cli.Connect(2, streams.STREAM_NETWORK_UDP) //创建udp网络

	if err != nil {
		slog.Error("客户端连接失败", slog.Any("err", err))
		return
	}
	slog.Info("客户端连接成功！", slog.Int("通道数", cli.ChannelCount))
	for i := 0; i < cli.ChannelCount; i++ {
		go receive(cli, i)
	}
	msg0 := "hello,i am channel 0 data from client"
	slog.Info("正在向通道0发送数据", slog.String("msg", msg0))
	_, _ = cli.Send(cli.Streams[0], []byte(msg0))
	msg1 := "hello,i am channel 1 data from client"
	slog.Info("正在向通道1发送数据", slog.String("msg", msg1))
	_, _ = cli.Send(cli.Streams[0], []byte(msg1))

}

func TestStreamChannel(t *testing.T) {
	channelCount := 3
	testChannelIndex := 2
	testData := streams.StreamChannelData{
		ChannelId: testChannelIndex,
		Offset:    0,
		Data:      []byte("test787899999999999999999999999999999999999999999999999999999999999999999999999997893445342432"),
	}

	var channels []*streams.StreamChannel
	channels = make([]*streams.StreamChannel, channelCount)
	fmt.Println("开始创建通道")
	for i := 0; i < 3; i++ {
		channels[i] = streams.NewStreamChannel(i) // make(streams.StreamChannel)
	}
	if channels[0].Channel != nil {
		fmt.Println("通道校验成功")
	}
	var currentBuffers []*streams.StreamChannelData
	currentBuffers = make([]*streams.StreamChannelData, channelCount)
	msg := fmt.Sprintf("缓存长度:%d", len(currentBuffers))
	fmt.Println(msg)

	//写入测试数据
	go func() {
		for i := 0; i < 100; i++ {
			channels[testChannelIndex].Channel <- testData
		}
	}()
	bufferMaxSize := 10
	cacheData := make([]byte, bufferMaxSize) // 创建字节切片
	totolSize := 0
	msgCount := 0
	for {
		if currentBuffers[testChannelIndex] == nil {
			buffer, ok := <-channels[2].Channel
			if !ok {
				fmt.Println("读取失败")
			}
			currentBuffers[testChannelIndex] = &buffer
		}
		bufferSize := len(currentBuffers[testChannelIndex].Data)

		copySize := min(bufferSize-currentBuffers[testChannelIndex].Offset, bufferMaxSize) //修改成根据缓冲区大小来读取数据

		copy(cacheData, currentBuffers[testChannelIndex].Data[currentBuffers[testChannelIndex].Offset:currentBuffers[testChannelIndex].Offset+copySize])
		//
		//io.CopyN(cacheData,)
		//
		//CopyBytes(unsafe.Pointer(&cacheData[0]), unsafe.Pointer(&currentBuffers[testChannelIndex].Data[0]),
		//	currentBuffers[testChannelIndex].Offset, copySize)

		//C.memcpy(unsafe.Pointer(&cacheData[0]), unsafe.Pointer(uintptr(unsafe.Pointer(&currentBuffers[testChannelIndex].Data[0]))+uintptr(currentBuffers[testChannelIndex].Offset)), C.size_t(copySize))
		totolSize += copySize
		msg = fmt.Sprintf("当前读取的字节长度为：%d,合计读取：%d,读取的内容：%s", copySize, totolSize, string(cacheData[:copySize]))
		fmt.Println(msg)
		currentBuffers[testChannelIndex].Offset += copySize
		if currentBuffers[testChannelIndex].Offset >= bufferSize {
			currentBuffers[testChannelIndex] = nil
			msgCount++
			if msgCount == 100 {
				break
			}
		}
	}

	fmt.Println("开始关闭通道")
	for i := 0; i < 3; i++ {
		close(channels[i].Channel)
		channels[i].Channel = nil
	}
	if currentBuffers[0] != nil { //当前缓存没有工作时
		fmt.Println("开始清除数据")
		currentBuffers[0] = nil
		currentBuffers = make([]*streams.StreamChannelData, 0)
	}
	if len(currentBuffers) == 0 {
		fmt.Println("校验清除完成！")
	}
	fmt.Println("执行完成")
}
