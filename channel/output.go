package channel

import (
	"fmt"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/gwuhaolin/livego/av"
)

// outputReader needs to implement ReadCloser and one more "sendChunk(Chunk)"
// method. The main() loop above with read a chunk from the source returned by
// selectSource() and then pass it to outputReader.sendChunk()
type outputReader struct {
	bufferHead  *packet
	bufferTail  *packet
	bufferMutex sync.Mutex
	queue       chan *av.Packet
	info        av.Info
	closed      bool
	closeError  error
	stop        bool
	waitGroup   sync.WaitGroup
	lastTs      uint32
	outputTs    uint32
}

func newOutputReader(app string, room string) *outputReader {
	return &outputReader{

		queue: make(chan *av.Packet, 100),
		info: av.Info{
			Key:   fmt.Sprintf("%s/%s", app, room),
			Inter: true,
		},
		closed:     false,
		closeError: nil,
		waitGroup:  sync.WaitGroup{},
		lastTs:     0,
		outputTs:   0,
	}
}

func (o *outputReader) start() {
	o.waitGroup.Add(1)
	defer func() {
		o.waitGroup.Done()
	}()
	time.Sleep(time.Second)
	startTime := time.Now().UnixMilli()

	ticker := time.NewTicker(5 * time.Millisecond)

	for {
		if o.closed {
			break
		}
		tick := <-ticker.C
		runtime := tick.UnixMilli() - startTime
		currentTime := uint32(runtime)
		for {
			if o.bufferHead == nil || o.bufferHead.p.TimeStamp > currentTime {
				break
			}
			p := o.bufferHead.p
			o.bufferMutex.Lock()
			next := o.bufferHead.next
			o.bufferHead = next
			o.bufferMutex.Unlock()

			is_meta := p.IsMetadata
			is_audio := p.IsAudio
			is_key := false
			if p.IsVideo {
				vh := p.Header.(av.VideoPacketHeader)
				is_key = vh.IsKeyFrame()

				is_critical := is_meta || is_audio || is_key

				if len(o.queue) < 80 || is_critical {
					o.queue <- p
				} else {
					log.Warningf("outputReader queue too full: %d (dropping video packet ts=%d)", len(o.queue), p.TimeStamp)
				}
			}

		}
	}

}

// sendChunk cleans metadata and returns both the new timestamp and the chunk
// duration.
func (o *outputReader) sendChunk(c *chunk, streamTimestamp uint32) (uint32, uint32) {
	p := c.startPacket
	var duration uint32 = 0
	for {
		if p == nil {
			return streamTimestamp + duration, duration
		}

		offset := p.p.TimeStamp - c.startTimestamp
		if offset > duration {
			duration = offset
		}
		o.sendPacket(p.p, c.startTimestamp+offset)
		// As we send the chunk we need to detect and rewrite metadata packets
		// so they contain correct stream values instead of file values
		// https://helpx.adobe.com/adobe-media-server/dev/adding-metadata-live-stream.html

		next := p.next
		p = next
	}
}

func (o *outputReader) sendPacket(p *av.Packet, streamTimestamp uint32) {
	packetCopy := &av.Packet{}
	dupePacket(*p, packetCopy, true, streamTimestamp)
	if packetCopy.IsMetadata {
		videoHdr, ok := packetCopy.Header.(av.VideoPacketHeader)
		if ok {
			ct := videoHdr.CompositionTime()
			log.Debugf("got header with composition time: %d", ct)
		}
	}

	pkt := &packet{p: packetCopy}
	if o.outputTs > pkt.p.TimeStamp {
		log.Warningf("old timestamp found: %d, already sent %d", pkt.p.TimeStamp, o.outputTs)
	}
	o.outputTs = pkt.p.TimeStamp
	defer func() {
		o.bufferMutex.Unlock()
	}()
	o.bufferMutex.Lock()
	if o.bufferHead == nil {
		o.bufferHead = pkt
		o.bufferTail = o.bufferHead
	} else {
		o.bufferTail.next = pkt
		o.bufferTail = pkt
	}
}

func (o *outputReader) Read(p *av.Packet) error {
	if len(o.queue) > 190 {
		log.Warningf("outputReader queue is at %d on read side", len(o.queue))
	}
	packetOut := <-o.queue
	dupePacket(*packetOut, p, false, 0)
	if o.lastTs > p.TimeStamp {
		log.Warningf("Timestamp out of order! lastTS=%d, packet Time=%d", o.lastTs, p.TimeStamp)
	}
	if p.TimeStamp > o.lastTs+100 {
		log.Warningf("100ms gap discovered! packet Time=%d, lastTS=%d", p.TimeStamp, o.lastTs)
	}
	o.lastTs = p.TimeStamp
	return nil
}

func (o *outputReader) Alive() bool {
	return !o.closed
}

func (o *outputReader) Info() av.Info {
	return o.info
}

func (o *outputReader) Close(err error) {
	log.Errorf("channel outputReader closed: %s", err)
	o.closeError = err
	o.closed = true
}

func dupePacket(in av.Packet, out *av.Packet, setTs bool, timeStamp uint32) {
	out.IsVideo = in.IsVideo
	out.IsAudio = in.IsAudio
	out.IsMetadata = in.IsMetadata
	out.StreamID = in.StreamID
	out.TimeStamp = in.TimeStamp
	out.Header = in.Header
	out.Data = in.Data
	if setTs {
		out.TimeStamp = timeStamp
	}
}
