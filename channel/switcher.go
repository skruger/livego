package channel

import (
	"github.com/gwuhaolin/livego/av"
	log "github.com/sirupsen/logrus"
)

type delaySwitcher struct {
	sources           ChannelSources
	bufferTimeMs      uint32
	selected          string
	timestamp         uint32
	measuredTimestamp uint32

	firstChunk *chunk
	lastChunk  *chunk
}

func newDelaySwitcher(bufferTimeMs uint32) *delaySwitcher {
	return &delaySwitcher{
		bufferTimeMs: bufferTimeMs,
	}
}

func (d *delaySwitcher) addSource(src ChannelSource) {
	d.sources = append(d.sources, src)
}

func (d *delaySwitcher) updateTime(timestamp uint32) {
	d.timestamp = timestamp
	//log.Infof("timestamp: %d, sources: %d", timestamp, len(d.sources))
	for _, source := range d.sources {
		if readyChunk := source.receiver.cm.pollFinishedChunk(); readyChunk != nil {
			readyChunk.next = nil
			readyChunk.relativeTimestamp = timestamp
			//log.Infof("selected = '%s'", d.selected)
			if d.selected == "" {
				d.selected = source.name
			}

			if d.selected == source.name {
				d.fixChunkTimestamps(readyChunk)
				metaPacket := &av.Packet{}
				// inject this source's metadata in every chunk
				if source.receiver.metadataPacket != nil {
					dupePacket(*source.receiver.metadataPacket, metaPacket, true, readyChunk.startTimestamp)
					mp := &packet{
						p:    metaPacket,
						next: readyChunk.startPacket,
					}
					readyChunk.startPacket = mp
				}

				d.appendChunk(readyChunk)
			}
		}
	}
}

func (d *delaySwitcher) appendChunk(c *chunk) {
	if d.firstChunk == nil {
		d.firstChunk = c
		d.lastChunk = c
	} else {
		d.lastChunk.next = c
		d.lastChunk = c
	}

}

func (d *delaySwitcher) fixChunkTimestamps(c *chunk) {
	if d.measuredTimestamp == 0 {
		d.measuredTimestamp = d.timestamp + d.bufferTimeMs
		log.Infof("initial timestamp: %d", d.measuredTimestamp)
	} else {
		log.Infof("set chunk timestamp to %d, current time: %d", d.measuredTimestamp, d.timestamp)
	}
	start := d.measuredTimestamp
	offset := c.resetTimestamp(start)
	//baseTimestamp := c.startTimestamp
	//c.startTimestamp = start
	//p := c.startPacket
	//var offset uint32 = 0
	//for {
	//	if p == nil {
	//		break
	//	}
	//	offset = p.p.TimeStamp - baseTimestamp
	//	p.p.TimeStamp = start + offset
	//	p = p.next
	//}
	d.measuredTimestamp = start + offset
	log.Infof("chunk added %d ending at %d", offset, d.measuredTimestamp)
}

func (d *delaySwitcher) isReady() bool {
	if d.lastChunk == nil {
		//log.Infof("no last chunk")
		return false
	}
	minTs := d.timestamp - 1500
	if minTs > d.firstChunk.startTimestamp {
		//log.Infof("isReady() false, minTs=%d lastChunk.startTimestamp=%d", minTs, d.lastChunk.startTimestamp)
		return false
	}
	return true
}

func (d *delaySwitcher) pollChunkReady() *chunk {
	//
	releaseTimestamp := d.firstChunk.relativeTimestamp + d.bufferTimeMs
	if releaseTimestamp < d.lastChunk.relativeTimestamp {
		c := d.firstChunk
		d.firstChunk = c.next
		c.next = nil
		if d.firstChunk == d.lastChunk {
			d.firstChunk = nil
			d.lastChunk = nil
			d.measuredTimestamp = 0
			log.Infof("all chunks depleated, resetting delaySwitcher")
		}
		log.Infof("chunk starts at %d", c.startTimestamp)
		return c
	}
	return nil
}

// Align does nothing for live streams. The timestamps are already aligned
func (d *delaySwitcher) Align(_ uint32) {
	return
}

func (d *delaySwitcher) Read(timestamp uint32) (*chunk, error) {
	if timestamp < d.timestamp {
		log.Warningf("going back in time %d -> %d", d.timestamp, timestamp)
	}
	d.updateTime(timestamp)
	if d.lastChunk == nil {
		//return nil, fmt.Errorf("no chunks in delaySwitcher")
		return nil, nil
	}
	c := d.pollChunkReady()
	return c, nil
}
