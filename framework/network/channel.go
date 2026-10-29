package network

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"github.com/DeleteElf/zero-net/framework"
	"github.com/DeleteElf/zero-net/framework/utils"
	"github.com/quic-go/quic-go"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"
)

type StreamChannelOperating interface {
	CreateChannels()
	HandleChannelStream(channelId int, stream *quic.Stream)
	Send(channelId int, data []byte) (bool, error)
}

type DataStream struct {
	ClientId  string
	ChannelId int
	Ssrc      uint8
	Offset    int
	Size      int
	Level     FecLevel
	Reader    io.Reader
}

type MessageChannelCallbackFunc func(string, int)
type FrameLostFunc func(uint8, uint32, uint32)

type StreamChannel struct {
	//Channel       chan StreamChannelData
	//Buffer        *StreamChannelData
	DataStreamChannel chan DataStream
	BufferStream      *DataStream

	ClientId  string
	ChannelId int
	Cancel    context.CancelFunc
	Done      bool

	Stream        *quic.Stream
	Level         FecLevel
	Depacketizers map[uint8]*Depacketizer
	Packetizers   map[uint8]*Packetizer
	lockFecGroups sync.Mutex

	OnConnect    MessageChannelCallbackFunc
	OnDisconnect MessageChannelCallbackFunc
	OnFrameLost  FrameLostFunc
	framework.CloseableObject
}

// NewStreamChannel 创新数据通道，并确定传输类型
//
//	-param id:数据通道的编号
//	-param index:数据通道的索引
//	-param c:数据通道的配置
//
// return:通道实例
func NewStreamChannel(id string, index int) *StreamChannel {
	slog.Debug("正在创建通道", slog.String("id", id), slog.Int("ChannelId", index))
	//cacheCount := 2
	//if index >= 2 {
	//	cacheCount = 60
	//}
	sc := &StreamChannel{
		//Channel:       make(chan StreamChannelData, 200), //这边队列会引起数据缓存堆叠，导致鼠标卡顿！
		DataStreamChannel: make(chan DataStream),
		ClientId:          id,
		ChannelId:         index,
		Depacketizers:     make(map[uint8]*Depacketizer), //初始化空的解包器
		Packetizers:       make(map[uint8]*Packetizer),   //初始化空的打包器
		CloseableObject: framework.CloseableObject{
			Closed: false,
		},
	}
	sc.Depacketizers[0] = NewDepacketizer(sc) //初始化一个解包器
	sc.Packetizers[0] = NewPacketizer()       //初始化一个打包器
	sc.SetOnCloseHandler(sc)
	return sc
}

func (sc *StreamChannel) OnClosing() bool {
	if sc.Cancel != nil {
		sc.Cancel()
	}
	sc.Cancel = nil
	if sc.Stream != nil {
		sc.Stream.CancelRead(0)
		_ = sc.Stream.Close()
		sc.Stream.CancelWrite(0)
		sc.Stream = nil
	}
	count := 100
	for i := 0; i < count; i++ {
		if !sc.Done {
			time.Sleep(time.Millisecond)
			continue
		}
		break
	}
	return true
}

func (sc *StreamChannel) OnClosed() error {
	slog.Debug("检测到通道已经退出！", slog.String("id", sc.ClientId), slog.Int("通道", sc.ChannelId))
	//sc.Buffer = nil
	sc.BufferStream = nil
	return nil
}

// HandleStreamData 从通道接收流的数据
func (sc *StreamChannel) HandleStreamData(stream *quic.Stream) {
	sc.Stream = stream
	_, sc.Cancel = context.WithCancel(sc.Stream.Context())
	defer func() {
		if !sc.Done && sc.DataStreamChannel != nil {
			close(sc.DataStreamChannel)
			sc.DataStreamChannel = nil
		}
		sc.Done = true
		if sc.OnDisconnect != nil {
			sc.OnDisconnect(sc.ClientId, sc.ChannelId)
		}
	}()
	slog.Debug("完成流与通道的对接，开始读取通道数据", slog.Int("channel", sc.ChannelId))
	if sc.OnConnect != nil {
		sc.OnConnect(sc.ClientId, sc.ChannelId)
	}
	for {
		if sc.IsClosed() {
			return
		}
		buf, err := utils.ReadStreamByHeaderUShort(sc.Stream)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) { //如果是读取超时，我们就继续即可
				continue
			} else if err != io.EOF {
				//信息太频繁，不用一直提示
				//slog.Error("通道读取失败！", slog.Int("ChannelId", sc.ChannelId), slog.Any("err", err))
			} else {
				//slog.Error("通道流已经结束！", slog.Int("ChannelId", sc.ChannelId))
			}
			return
		}
		if sc.IsClosed() {
			return
		}
		size := len(buf)
		if size == 0 { //读取到0长度的数据包，我们认为是断开连接了
			return
		}
		if sc == nil {
			return
		}
		sc.handleStreamReaderToChannel(bytes.NewReader(buf), size)
	}
}

func (sc *StreamChannel) ReceiveDataToStreamReader() bool {
	if sc.BufferStream == nil {
		stream, ok := <-sc.DataStreamChannel
		if !ok {
			slog.Warn("通道已经关闭！", slog.Any("ChannelId", sc.ChannelId))
			return ok
		}
		sc.BufferStream = &stream
	}
	return true
}

func (sc *StreamChannel) Send(data []byte) (bool, error) {
	if sc.IsClosed() {
		return false, nil
	}
	if sc.Stream == nil {
		return false, nil
	}
	err := utils.WriteStreamByHeaderUShort(sc.Stream, data)
	return err == nil, err
}

func (sc *StreamChannel) notifyFrameLost(ssrc uint8, frameIndex uint32, speculative bool) {
	//todo:notifyFrameLost(trackIndex,queue->currentFrameNumber, true);
	depacketizer := sc.Depacketizers[ssrc]
	if !depacketizer.WaitingForIdrFrame {
		depacketizer.WaitingForIdrFrame = true
		message := "发送针对不可恢复帧的RFI请求"
		if speculative {
			message = "针对预测的帧丢失发送推测性信息请求（RFI）"
		}
		slog.Debug(message, slog.Any("ssrc", ssrc), slog.Any("frameIndex", frameIndex))
		startFrameIndex := depacketizer.CurrentFrameIndex
		if sc.OnFrameLost != nil { //方案1：回调
			sc.OnFrameLost(ssrc, startFrameIndex, frameIndex)
		} else { //方案2：自己发
			sc.connectionDetectedFrameLoss(ssrc, startFrameIndex, frameIndex)
		}
	}
}

func (sc *StreamChannel) connectionDetectedFrameLoss(ssrc uint8, start, end uint32) {
	sc.requestRfiFrame(ssrc, start, end)
}

// 用来通知主机，已经完成的帧，当我们接收到一个帧时，更新当前帧的编号，如果该帧是LTR（丢失传输请求），则发送ACK（确认）控制消息
func (sc *StreamChannel) connectionReceivedCompleteFrame(ssrc uint8, start uint32, frameIsLTR bool) {
	//setLastGoodFrame(trackIndex,frameIndex);
	if frameIsLTR { //LTR_ACK 告诉主机，此帧可以参考
		//todo:这个暂时先有上层逻辑显示,存在lbq的逻辑
		data := make([]byte, 15)                        //如果加上头 应该是15
		binary.LittleEndian.PutUint16(data[0:], 0x0350) //SS_LTR_FRAME_ACK_PTYPE
		binary.LittleEndian.PutUint32(data[2:], uint32(ssrc))
		binary.LittleEndian.PutUint32(data[6:], start)
		//binary.LittleEndian.PutUint32(data[10:], 0)
		//data[14] = 0x00 //Rfi
		_, _ = sc.Send(data)
		slog.Debug("发送LTR_ACK", slog.Any("ssrc", ssrc), slog.Any("startFrameIndex", start))
	}
}

// 请求rfi参考帧
func (sc *StreamChannel) requestRfiFrame(ssrc uint8, start, end uint32) {
	if !utils.IsBefore32(end, start) { //start<=end
		data := make([]byte, 26)
		binary.LittleEndian.PutUint16(data[0:], 0x0301) //IDX_INVALIDATE_REF_FRAMES
		if start < 0x20 {
			binary.LittleEndian.PutUint32(data[2:], 0)
			binary.LittleEndian.PutUint32(data[10:], start)
		} else {
			binary.LittleEndian.PutUint32(data[2:], start)
			binary.LittleEndian.PutUint32(data[10:], end)
		}
		binary.LittleEndian.PutUint32(data[18:], uint32(ssrc))
		_, _ = sc.Send(data)
		slog.Debug("发送Rfi帧申请成功", slog.Any("ssrc", ssrc), slog.Any("startFrameIndex", start), slog.Any("endFrameIndex", end))
	} else {
		slog.Debug("发送Rfi帧申请失败，start大于end", slog.Any("ssrc", ssrc), slog.Any("startFrameIndex", start), slog.Any("endFrameIndex", end))
	}
}

// 请求关键帧
func (sc *StreamChannel) requestIdrFrame(ssrc uint8) {
	depacketizer := sc.Depacketizers[ssrc]
	depacketizer.WaitingForIdrFrame = true //标记成等待关键帧
	data := make([]byte, 4)
	binary.LittleEndian.PutUint16(data[0:], 0x0302)       //报头 IDX_REQUEST_IDR_FRAME
	binary.LittleEndian.PutUint16(data[2:], uint16(ssrc)) //start
	_, _ = sc.Send(data)
	slog.Debug("发送Idr 关键帧请求", slog.Any("ssrc", ssrc))
}

// handleStreamReaderToChannel 从quic的标准数据流接收数据
func (sc *StreamChannel) handleStreamReaderToChannel(r io.Reader, size int) {
	sc.handleReaderToChannel(0, r, size)
}
func (sc *StreamChannel) handleReaderToChannel(ssrc uint8, r io.Reader, size int) {
	if sc.DataStreamChannel != nil {
		sc.DataStreamChannel <- DataStream{
			ClientId:  sc.ClientId,
			ChannelId: sc.ChannelId,
			Offset:    0,
			Size:      size,
			Ssrc:      ssrc,
			Level:     sc.Level,
			Reader:    r,
		}
	}
}
func (sc *StreamChannel) handleNetBufferToChannel(ssrc uint8, b net.Buffers, size int) {
	if sc.DataStreamChannel != nil {
		sc.DataStreamChannel <- DataStream{
			ClientId:  sc.ClientId,
			ChannelId: sc.ChannelId,
			Offset:    0,
			Size:      size,
			Ssrc:      ssrc,
			Level:     sc.Level,
			Reader:    &b,
		}
	}
}
