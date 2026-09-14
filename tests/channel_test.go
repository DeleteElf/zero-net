package tests

import (
	"bytes"
	"fmt"
	"github.com/DeleteElf/zero-net/framework/network"
	"io"
	"testing"
)

func TestStreamChannel(t *testing.T) {
	channelCount := 3
	testChannelIndex := 2
	data := []byte("test787899999999999999999999999999999999999999999999999999999999999999999999999997893445342432")
	var channels []*network.StreamChannel
	channels = make([]*network.StreamChannel, channelCount)
	fmt.Println("开始创建通道")
	for i := 0; i < 3; i++ {
		channels[i] = network.NewStreamChannel("test001", i) // make(streams.DataStreamChannel)
	}
	if channels[0].Stream != nil {
		fmt.Println("通道校验成功")
	}
	var currentBuffers []*network.DataStream
	currentBuffers = make([]*network.DataStream, channelCount)
	msg := fmt.Sprintf("缓存长度:%d", len(currentBuffers))
	fmt.Println(msg)

	//写入测试数据
	go func() {
		for i := 0; i < 100; i++ {
			msg = fmt.Sprintf("正在写入第%d条", i)
			fmt.Println(msg)
			channels[testChannelIndex].DataStreamChannel <- network.DataStream{
				ChannelId: testChannelIndex,
				Offset:    0,
				Size:      len(data),
				Reader:    bytes.NewReader(data), //   []byte("test787899999999999999999999999999999999999999999999999999999999999999999999999997893445342432"),
			}

		}
	}()
	bufferMaxSize := 10
	cacheData := make([]byte, bufferMaxSize) // 创建字节切片
	totolSize := 0
	msgCount := 0
	for {
		if currentBuffers[testChannelIndex] == nil {
			stream, ok := <-channels[2].DataStreamChannel
			if !ok {
				fmt.Println("读取失败")
			}
			currentBuffers[testChannelIndex] = &stream
			fmt.Println("创建缓存，准备分段读取！")
		}
		stream := currentBuffers[testChannelIndex]
		copySize := min(stream.Size-stream.Offset, bufferMaxSize) //修改成根据缓冲区大小来读取数据
		readCount, err := io.ReadAtLeast(stream.Reader, cacheData, copySize)
		if err != nil {
			return
		}
		stream.Offset += readCount
		//copy(cacheData, stream.Data[stream.Offset:stream.Offset+copySize])
		//
		//io.CopyN(cacheData,)
		//
		//CopyBytes(unsafe.Pointer(&cacheData[0]), unsafe.Pointer(&stream.Data[0]),
		//	stream.Offset, copySize)

		//C.memcpy(unsafe.Pointer(&cacheData[0]), unsafe.Pointer(uintptr(unsafe.Pointer(&stream.Data[0]))+uintptr(stream.Offset)), C.size_t(copySize))
		totolSize += copySize
		msg = fmt.Sprintf("当前读取的字节长度为：%d,合计读取：%d,读取的内容：%s", copySize, totolSize, string(cacheData[:copySize]))
		fmt.Println(msg)
		//stream.Offset += copySize
		if stream.Offset >= stream.Size {
			currentBuffers[testChannelIndex] = nil
			msgCount++
			if msgCount == 100 {
				break
			}
		}
	}

	fmt.Println("开始关闭通道")
	for i := 0; i < 3; i++ {
		close(channels[i].DataStreamChannel)
		channels[i].DataStreamChannel = nil
	}
	if currentBuffers[0] != nil { //当前缓存没有工作时
		fmt.Println("开始清除数据")
		currentBuffers[0] = nil
		currentBuffers = make([]*network.DataStream, 0)
	}
	if len(currentBuffers) == 0 {
		fmt.Println("校验清除完成！")
	}
	fmt.Println("执行完成")
}
