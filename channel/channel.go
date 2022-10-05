package channel

import (
	"fmt"
	"github.com/gwuhaolin/livego/av"
	"github.com/gwuhaolin/livego/configure"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

const (
	stream_preroll = iota
	stream_live    = iota
	stream_end     = iota
)

type ChannelEvent struct {
	action string
}

type ChannelSource struct {
	name     string
	receiver *av.WriteCloser
	appname  string
	room     string
	cs       *ChannelStream
}

type ChannelSources []ChannelSource

func (c *ChannelSources) findByName(name string) *ChannelSource {
	for _, source := range *c {
		if source.name == name {
			return &source
		}
	}
	return nil
}

type StaticSource struct {
	name       string
	firstChunk *chunk
	cs         *ChannelStream
}

type StaticSources []StaticSource

func (s *StaticSources) findByName(name string) *StaticSource {
	for _, source := range *s {
		if source.name == name {
			return &source
		}
	}
	return nil
}

type ChannelState struct {
	name         string
	appName      string
	config       *configure.Channel
	ch           chan ChannelEvent
	stream_state int
	stopping     bool
	liveSources  ChannelSources
	//slateSources StaticSources
	staticAssets      staticAssets
	timestamp         uint32
	streamedTimestamp uint32
	beginCallback     BeginOutputCallback
	outputReader      *outputReader
}

var channels = &sync.Map{}
var serverReady = false

func ListChannels() (ret []string) {
	channels.Range(func(key interface{}, val interface{}) bool {
		ret = append(ret, key.(string))
		return true
	})
	return ret
}

func SetServerReady(ready bool) {
	serverReady = ready
}

// waitServerReady works with SetServerReady to synchronize channel startup
// so it happens after the rtmp server is running. This makes sure the output
// callbacks are registered and that the server is ready for the callbacks
// to be called when the channel starts.
func waitServerReady() {
	for {
		if serverReady {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func GetChannelSourceWriteClosers(streamKey string) ([]*av.WriteCloser, error) {
	var closers []*av.WriteCloser
	var err error
	channels.Range(func(key interface{}, val interface{}) bool {
		if channel, ok := val.(*ChannelState); ok {
			for _, chanSource := range channel.liveSources {
				sourceKey := fmt.Sprintf("%s/%s", chanSource.appname, chanSource.room)
				if sourceKey == streamKey {
					closers = append(closers, chanSource.receiver)
				}
			}
		}
		return true
	})
	return closers, err
}

func SetBeginOutputCallbacks(callback BeginOutputCallback) {
	channels.Range(func(key interface{}, val interface{}) bool {
		if channel, ok := val.(*ChannelState); ok {
			channel.beginCallback = callback
		}
		return true
	})
}

func StartChannel(appName string, name string) (*ChannelState, error) {
	c, ok := channels.Load(name)
	if ok {
		if oldChannel, ok := c.(*ChannelState); ok {
			return oldChannel, nil
		}
	}
	chanCfg, err := configure.GetChannelCfg(name)
	if err != nil {
		chanCfg = &configure.Channel{Name: name}
	}

	newChannel := &ChannelState{
		name:         name,
		appName:      appName,
		config:       chanCfg,
		ch:           make(chan ChannelEvent, 100),
		stream_state: stream_preroll,
	}
	for _, rtmpSource := range chanCfg.RtmpSources {
		writeCloser := NewStreamSource()
		cs := ChannelSource{
			name:     rtmpSource.Name,
			receiver: &writeCloser,
			appname:  rtmpSource.App,
			room:     rtmpSource.Room,
		}
		log.Infof("bound channel source for %s/%s to %s/%s", name, rtmpSource.Name, rtmpSource.App, rtmpSource.Room)
		newChannel.liveSources = append(newChannel.liveSources, cs)
	}

	for _, fileSource := range chanCfg.StaticSources {
		asset, err := newStaticAsset(fileSource)
		if err != nil {
			return nil, err
		}
		newChannel.staticAssets = append(newChannel.staticAssets, asset)
	}

	channels.Store(name, newChannel)
	go newChannel.main()
	return newChannel, nil
}

func StopChannel(name string) {
	c, ok := channels.Load(name)
	if !ok {
		return
	}
	if channel, ok := c.(*ChannelState); ok {
		channel.SendEvent(ChannelEvent{action: "stop"})
		channels.Delete(name)
	}
}

func (c *ChannelState) SendEvent(event ChannelEvent) {
	c.ch <- event
}

func (c *ChannelState) timer() {
	startTime := time.Now().UnixMilli()
	ticker := time.NewTicker(500 * time.Millisecond)
	for {
		if c.stopping {
			break
		}
		tick := <-ticker.C
		runtime := tick.UnixMilli() - startTime
		c.timestamp = uint32(runtime)
		c.SendEvent(ChannelEvent{action: "tick"})
	}
}

func (c *ChannelState) main() {
	waitServerReady()
	c.outputReader = newOutputReader(c.appName, c.name)
	c.beginCallback(c.outputReader)
	oldSource := ""
	go c.timer()
	c.streamedTimestamp = 0
	for {
		// Receive on event channel
		ev := <-c.ch
		switch ev.action {
		case "stop":
			log.Info("channel %s stopping", c.name)
			c.stopping = true
			break
		case "tick":
			name, source := c.selectSource()
			if name != oldSource {
				oldSource = name
				log.Infof("channel %s, tick %d, switched to source %s", c.name, c.timestamp, name)
				// When selecting a new source an onMetaData packet needs to be sent according
				// to the onMetaData section starting on page 14 of Video File Format Spec v10
				// perhaps send it every chunk (each 'I' and following 'P' frames)
			}
			for {
				currentChunk, err := source.Read(c.timestamp)
				if err != nil {
					log.Errorf("error reading chunk from source: %s", err)
				}
				if currentChunk == nil {
					break
				}
				// source needs to be able to return a start time that can be passed
				// to getDuration. getDuration then will find the videoPacketHeader
				// with the highest CompositionTime() and that will be used to get
				// the delta for the duration of the currentChunk

				// add chunk.cleanMeta() method to find metadata packet and strip
				// any values that only apply to files and not streams.
				var chunkDuration uint32 = 0
				c.streamedTimestamp, chunkDuration = c.outputReader.sendChunk(currentChunk, c.streamedTimestamp)
				log.Infof("sendChunk: timestamp=%d, duration=%d", c.streamedTimestamp, chunkDuration)

				if currentChunk.isFinal() {
					log.Infof("current chunk was final. aligning stream at new timestamp: %d", c.streamedTimestamp)
					source.Align(c.streamedTimestamp)
				}
				//log.Infof("read returned chunk: %w, error: %s", currentChunk, err)
			}
			// If we are here the clock sent a tick event and should have updated c.timestamp
		default:
			log.Warning("unrecognized channel event action '%s' on channel %s", ev.action, c.name)
		}
	}
}

// Selecting the correct live stream is a combination of looking at the
// currently selected stream and seeing if it has chunks that are ready
// If no chunks are ready we fall back to the current slate.
// There is a sliding window for the source and if it falls more than 3s
// behind then we fall back to slate. The stream is already buffering for
// N seconds so being off by up to 3 seconds is no big deal.

// As we record a chunk for live sources we mark the output stream time and then
// mark the time when it will be ready for reading at the end of the queue. As
// long as the output time keeps the 3 second buffer behind live no slate is
// needed.

// selectSource() should always be safe to call. It evaluates the current
// slate state and does the timing evaluations for the currently selected
// rtmp steam source. At each tick it will return the appropriate
// ChannelStream whose internal state is unchanged and can be .Read()
// until nothing else is returned for the current timestamp.
func (c *ChannelState) selectSource() (string, ChannelStream) {
	if c.stream_state == stream_preroll {
		slateSource := c.staticAssets.findByName(c.config.PrerollSlate)
		return slateSource.Name, slateSource
	}
	return "", nil
}

// Generate time events for stream transit
// Work in chunk units
// each tick updates the timestamp and we try to Read chunks from a ChannelStream
// When read returns nil chunk we continue and wait for the next time event
// Add a timer main loop that generates an event with action=tick while
// updating c.timestamp

// outputReader needs to implement ReadCloser and one more "sendChunk(Chunk)"
// method. The main() loop above with read a chunk from the source returned by
// selectSource() and then pass it to outputReader.sendChunk()
type outputReader struct {
	queue      chan *av.Packet
	info       av.Info
	closed     bool
	closeError error
}

func newOutputReader(app string, room string) *outputReader {
	return &outputReader{
		queue: make(chan *av.Packet, 500),
		info: av.Info{
			Key:   fmt.Sprintf("%s/%s", app, room),
			Inter: true,
		},
		closed:     false,
		closeError: nil,
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
		// As we send the chunk we need to detect and rewrite metadata packets
		// so they contain correct stream values instead of file values
		// https://helpx.adobe.com/adobe-media-server/dev/adding-metadata-live-stream.html

		// check video metadata and update highestCompositionTime using
		// VideoPacketHeader.CompositionTime() cast to uint32
		packetCopy := &av.Packet{}
		// Double copying isn't great, but we don't want to modify the timestamp
		// ini the original source. It would break static slate assets in memory.
		dupePacket(*p.p, packetCopy, streamTimestamp+offset)
		if packetCopy.IsMetadata {
			videoHdr, ok := packetCopy.Header.(av.VideoPacketHeader)
			if ok {
				ct := videoHdr.CompositionTime()
				log.Infof("got header with composition time: %d", ct)
			}
		}
		o.queue <- packetCopy
		next := p.next
		p = next
	}
}

func (o *outputReader) Read(p *av.Packet) error {
	packetOut := <-o.queue
	dupePacket(*packetOut, p, 0)
	return nil
}

func (o *outputReader) Alive() bool {
	return !o.closed
}

func (o *outputReader) Info() av.Info {
	return o.info
}

func (o *outputReader) Close(err error) {
	o.closeError = err
	o.closed = true
}

func dupePacket(in av.Packet, out *av.Packet, timeStamp uint32) {
	out.IsVideo = in.IsVideo
	out.IsAudio = in.IsAudio
	out.IsMetadata = in.IsMetadata
	out.StreamID = in.StreamID
	out.TimeStamp = in.TimeStamp
	out.Header = in.Header
	out.Data = in.Data
	if timeStamp > 0 {
		out.TimeStamp = timeStamp
	}
}
