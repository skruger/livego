package channel

import (
	"fmt"
	"github.com/gwuhaolin/livego/av"
)

// Packet and chunk linked lists are for holding stream information
// in memory while playing

type packet struct {
	p    *av.Packet
	next *packet
}

type chunk struct {
	startTimestamp uint32
	startPacket    *packet
	endPacket      *packet
	next           *chunk
}

func (c *chunk) addPacket(p *av.Packet) {
	cPacket := &packet{
		p:    p,
		next: nil,
	}
	c.endPacket.next = cPacket
	c.endPacket = cPacket
}

// cleanMetaData find metadata packets and replaces them with basic stream values
func (c *chunk) cleanMetaData() {
	return
}

// getDuration returns chunk time in milliseconds for advancing the timestamp clock
func (c *chunk) getDuration() int {
	return 0
}

func newChunk(p *av.Packet) *chunk {
	cPacket := &packet{
		p:    p,
		next: nil,
	}
	return &chunk{
		startTimestamp: p.TimeStamp,
		startPacket:    cPacket,
		endPacket:      cPacket,
	}
}

type chunkMaker struct {
	currentChunk *chunk
	firstChunk   *chunk
	chunkCount   int
}

func (m *chunkMaker) addPacket(p *av.Packet) *chunk {
	if m.currentChunk == nil {
		m.currentChunk = newChunk(p)
		m.firstChunk = m.currentChunk
		m.chunkCount = 1
		return m.currentChunk
	}
	// detect I-frame and start a new chunk or append the packet
	var vh av.VideoPacketHeader
	if p.IsVideo {
		vh = p.Header.(av.VideoPacketHeader)
		if vh.IsKeyFrame() && p.TimeStamp > m.currentChunk.startTimestamp {
			nextChunk := newChunk(p)
			m.currentChunk.next = nextChunk
			m.currentChunk = nextChunk
			m.chunkCount += 1
			return m.currentChunk
		}
	}
	m.currentChunk.addPacket(p)
	return nil
}

func (m *chunkMaker) loadSlate(closer av.ReadCloser) (int, error) {
	if closer == nil {
		return 0, fmt.Errorf("ReadCloser can not be null")
	}
	var packetCount int = 0
	for {
		p := &av.Packet{}
		err := closer.Read(p)
		if err != nil {
			break
		}
		m.addPacket(p)
		packetCount += 1
	}
	return packetCount, nil
}

type chunkPlayer struct {
	currentChunk  *chunk
	currentPacket *packet
}
