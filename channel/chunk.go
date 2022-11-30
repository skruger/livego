package channel

import (
	"fmt"
	"github.com/gwuhaolin/livego/av"
	"sync"
)

// Packet and chunk linked lists are for holding stream information
// in memory while playing

type packet struct {
	p    *av.Packet
	next *packet
}

type foundMetadataCallback func(p *av.Packet)

type chunk struct {
	startTimestamp    uint32
	duration          uint32
	relativeTimestamp uint32
	startPacket       *packet
	endPacket         *packet
	next              *chunk
	sourceTag         string
}

func (c *chunk) addPacket(p *av.Packet) {
	cPacket := &packet{
		p:    p,
		next: nil,
	}
	duration := p.TimeStamp - c.startTimestamp
	if duration > c.duration {
		c.duration = duration
	}
	c.endPacket.next = cPacket
	c.endPacket = cPacket
}

func (c *chunk) isFinal() bool {
	return c.next == nil
}

func (c *chunk) resetTimestamp(newTimestamp uint32) uint32 {
	baseTimestamp := c.startTimestamp
	start := newTimestamp
	c.startTimestamp = start
	p := c.startPacket
	var offset uint32 = 0
	for {
		if p == nil {
			break
		}
		offset = p.p.TimeStamp - baseTimestamp
		p.p.TimeStamp = start + offset
		p = p.next
	}
	return offset
}

func newChunk(p *av.Packet, lastTimestamp uint32, sourceTag string) *chunk {
	cPacket := &packet{
		p:    p,
		next: nil,
	}
	timestamp := lastTimestamp
	if timestamp == 0 {
		timestamp = p.TimeStamp
	}
	return &chunk{
		startTimestamp: timestamp,
		startPacket:    cPacket,
		endPacket:      cPacket,
		sourceTag:      sourceTag,
	}
}

type chunkMaker struct {
	currentChunk *chunk
	firstChunk   *chunk
	chunkCount   int
	lock         sync.Mutex
	metadataCb   foundMetadataCallback
	sourceTag    string
}

func (m *chunkMaker) setMetadataCallback(cb foundMetadataCallback) {
	m.metadataCb = cb
}

func (m *chunkMaker) addPacket(p *av.Packet) *chunk {
	if p.IsMetadata {
		if m.metadataCb != nil {
			m.metadataCb(p)
		}
	}
	defer func() {
		m.lock.Unlock()
	}()
	m.lock.Lock()
	if m.currentChunk == nil {
		m.currentChunk = newChunk(p, 0, m.sourceTag)
		m.firstChunk = m.currentChunk
		m.chunkCount = 1
		return m.currentChunk
	}
	// detect I-frame and start a new chunk or append the packet
	var vh av.VideoPacketHeader
	if p.IsVideo {
		vh = p.Header.(av.VideoPacketHeader)
		if vh.IsKeyFrame() && p.TimeStamp > m.currentChunk.startTimestamp {
			nextChunk := newChunk(p, m.currentChunk.startTimestamp+m.currentChunk.duration, m.sourceTag)
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

func (m *chunkMaker) pollFinishedChunk() *chunk {
	defer func() {
		m.lock.Unlock()
	}()
	m.lock.Lock()

	if m.firstChunk == nil || m.currentChunk == nil {
		return nil
	}
	if m.firstChunk != m.currentChunk {
		rChunk := m.firstChunk
		m.firstChunk = rChunk.next
		return rChunk
	}
	return nil
}

type chunkPlayer struct {
	currentChunk  *chunk
	currentPacket *packet
}
