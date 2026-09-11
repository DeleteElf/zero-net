package network

import (
	"fmt"
	"github.com/klauspost/reedsolomon"
	"sync"
)

type FecEncoderFactory struct {
	FecEncoders  map[string]reedsolomon.Encoder
	lockEncoders sync.Mutex
}

func (f *FecEncoderFactory) GetFecEncoder(dataShards, parityShards uint8) (reedsolomon.Encoder, error) {
	if dataShards > 0 && parityShards > 0 {
		key := fmt.Sprintf("%d_%f", dataShards, parityShards)
		f.lockEncoders.Lock()
		defer f.lockEncoders.Unlock()
		if f.FecEncoders[key] == nil {
			encoder, err := reedsolomon.New(int(dataShards), int(parityShards))
			if err != nil {
				return nil, err
			}
			f.FecEncoders[key] = encoder
		}
		return f.FecEncoders[key], nil
	}
	return nil, nil
}

// Packetizer 打包器
type Packetizer struct {
	FrameIndex  uint32
	BlockIndex  uint8
	GroupIndex  uint8
	PacketIndex uint8

	FecEncoderFactory
	//仅用于静态打包
	SharedShards [][]byte
	//仅用于静态打包
	ParityShards [][]byte
}

func NewPacketizer() *Packetizer {
	return &Packetizer{
		FecEncoderFactory: FecEncoderFactory{
			FecEncoders: make(map[string]reedsolomon.Encoder),
		},
	}
}
