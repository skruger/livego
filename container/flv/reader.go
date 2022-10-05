package flv

import (
	"bytes"
	"fmt"
	"github.com/gwuhaolin/livego/utils/pio"
	"github.com/gwuhaolin/livego/utils/uid"
	log "github.com/sirupsen/logrus"
	"os"
	"time"

	"github.com/gwuhaolin/livego/av"
)

type FLVReader struct {
	Uid string
	av.RWBaser
	//app, title, url string
	buf          []byte
	closed       chan struct{}
	ctx          *os.File
	closedWriter bool
	demuxer      *Demuxer
}

func NewFLVReader(ctx *os.File) (*FLVReader, error) {
	ret := &FLVReader{
		Uid:     uid.NewId(),
		ctx:     ctx,
		RWBaser: av.NewRWBaser(time.Second * 10),
		closed:  make(chan struct{}),
		buf:     make([]byte, headerLen),
		demuxer: NewDemuxer(),
	}

	headerBytes := make([]byte, 13)

	byteCount, err := ret.ctx.Read(headerBytes)

	if err != nil {
		return nil, fmt.Errorf("problem reading FLV header: %s", err)
	}

	if byteCount < 13 {
		return nil, fmt.Errorf("unable to read full header")
	}

	if bytes.Compare(headerBytes[:len(flvHeader)], flvHeader) != 0 {
		return nil, fmt.Errorf("file does not start with FLV header")
	}

	return ret, nil
}

func (reader *FLVReader) Read(p *av.Packet) error {
	h := reader.buf[:headerLen]
	if _, err := reader.ctx.Read(h); err != nil {
		return fmt.Errorf("unable to read FLV file packet header: %s", err)
	}
	typeID := pio.U8(h[0:1])
	dataLen := pio.I24BE(h[1:4])
	timestampBase := pio.I24BE(h[4:7])
	timestampExt := pio.U8(h[7:8])
	streamID := pio.I24BE(h[8:11])

	timestamp := (int32(timestampExt) << 24) | timestampBase
	reader.RWBaser.RecTimeStamp(p.TimeStamp, uint32(typeID))

	data := make([]byte, dataLen)
	count, err := reader.ctx.Read(data)
	if err != nil {
		return fmt.Errorf("could not read full packet: %s", err)
	}
	if int32(count) != dataLen {
		return fmt.Errorf("expected %d bytes, but got %d bytes instead", dataLen, count)
	}

	p.Data = data

	p.IsAudio = typeID == av.TAG_AUDIO
	p.IsVideo = typeID == av.TAG_VIDEO
	p.IsMetadata = typeID == av.TAG_SCRIPTDATAAMF0 || typeID == av.TAG_SCRIPTDATAAMF3
	p.StreamID = uint32(streamID)
	p.TimeStamp = uint32(timestamp)

	endBytes := make([]byte, 4)
	endByteCount, err := reader.ctx.Read(endBytes)
	if err != nil {
		log.Warnf("Unable to read end of packet bytes, got %d of 4 with error: %s", endByteCount, err)
	}

	return reader.demuxer.DemuxH(p)
}

func (reader *FLVReader) Alive() bool {
	// Return False at end of file
	return true
}

func (reader *FLVReader) Info() av.Info {
	return av.Info{
		UID: reader.Uid,
	}
}

func (reader *FLVReader) Close(error) {

}
