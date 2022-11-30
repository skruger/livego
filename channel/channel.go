package channel

import (
	"fmt"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/gwuhaolin/livego/av"
	"github.com/gwuhaolin/livego/configure"
)

const (
	source_none   = iota
	source_slate  = iota
	source_stream = iota

	slate_preroll = iota
	slate_live    = iota
	slate_end     = iota
)

type ChannelEvent struct {
	action string
}

type ChannelSource struct {
	name     string
	receiver *streamSource
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
	name    string
	appName string
	config  *configure.Channel
	wg      sync.WaitGroup

	stream_state      int
	streamClock       chan uint32 // server time source (100ms ticks)
	timestamp         uint32      // server time, updated when streamClock tick is sent
	streamedTimestamp uint32      // end time of all played chunks
	slateFillTimeout  uint32      // default 500ms past streamedTimestamp fill with slate
	playoutOffset     uint32      // default 1000ms
	// if timestamp > streamedTimestamp + slateFillTimeout transition to slate state
	// if any stream chunks are available transition to stream state

	ch       chan ChannelEvent
	stopping bool
	switcher *delaySwitcher
	slate    *slateProvider
	//slateSources StaticSources
	beginCallback BeginOutputCallback
	outputReader  *outputReader
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

func GetChannelSourceWriteClosers(streamKey string) ([]av.WriteCloser, error) {
	var closers []av.WriteCloser
	var err error
	channels.Range(func(key interface{}, val interface{}) bool {
		if channel, ok := val.(*ChannelState); ok {
			for _, chanSource := range channel.switcher.sources {
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
		name:             name,
		appName:          appName,
		config:           chanCfg,
		wg:               sync.WaitGroup{},
		ch:               make(chan ChannelEvent, 100),
		streamClock:      make(chan uint32, 10),
		stream_state:     source_none,
		slateFillTimeout: 500,
		playoutOffset:    1000,
		switcher:         newDelaySwitcher(7000),
		slate:            newSlateProvider(),
		stopping:         false,
	}
	for _, rtmpSource := range chanCfg.RtmpSources {
		writeCloser := newStreamSource(fmt.Sprintf("stream/%s", rtmpSource.Name))
		cs := ChannelSource{
			name:     rtmpSource.Name,
			receiver: writeCloser,
			appname:  rtmpSource.App,
			room:     rtmpSource.Room,
		}
		log.Infof("bound channel source for %s/%s to %s/%s", name, rtmpSource.Name, rtmpSource.App, rtmpSource.Room)
		newChannel.switcher.addSource(cs)
	}

	for _, fileSource := range chanCfg.StaticSources {
		asset, err := newStaticAsset(fileSource)
		if err != nil {
			return nil, err
		}
		newChannel.slate.addSlate(asset)
	}
	if len(newChannel.slate.staticAssets) == 0 {
		return nil, fmt.Errorf("channel '%s' can not start without any slate assets", newChannel.name)
	}

	slateErr := newChannel.slate.setSlate(chanCfg.PrerollSlate)
	log.Errorf("using '%s' instead of configured preroll slate: %s", newChannel.slate.currentSlate.Name, slateErr)

	channels.Store(name, newChannel)
	go func() {
		newChannel.wg.Add(1)
		defer func() {
			newChannel.wg.Done()
			newChannel.stopping = true
		}()
		newChannel.main()
	}()

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
	ticker := time.NewTicker(400 * time.Millisecond)
	for {
		if c.stopping {
			break
		}
		tick := <-ticker.C
		runtime := tick.UnixMilli() - startTime
		c.timestamp = uint32(runtime)
		c.streamClock <- c.timestamp
	}
}

func (c *ChannelState) main() {
	waitServerReady()
	c.outputReader = newOutputReader(c.appName, c.name)
	c.beginCallback(c.outputReader)
	//oldSource := ""
	go c.timer()
	go c.outputReader.start()
	c.streamedTimestamp = 0
	for {
		// Receive on event channel
		select {
		case tick := <-c.streamClock:
			c.switcher.updateTime(c.streamedTimestamp)
			if c.timestamp > c.streamedTimestamp {
				// look for next chunk
				//log.Infof("timestamp=%d, tick=%d, streamedTs=%d, slateTimeout=%d", c.timestamp, tick, c.streamedTimestamp, c.slateFillTimeout)
				if c.timestamp > c.streamedTimestamp+c.slateFillTimeout {
					c.handleSlate(tick)
				} else {
					c.handleStream(tick)
				}
			}
		case ev := <-c.ch:

			switch ev.action {
			case "stop":
				log.Info("channel %s stopping", c.name)
				c.stopping = true
				break
			default:
				log.Warning("unrecognized channel event action '%s' on channel %s", ev.action, c.name)
			}

		}
	}
}

func (c *ChannelState) addChunk(segment *chunk, meta *av.Packet) {
	if meta != nil {
		c.outputReader.sendPacket(meta, c.streamedTimestamp)
	}

	segment.resetTimestamp(c.streamedTimestamp)

	chunkPackets := 0
	next := segment.startPacket
	minTs := next.p.TimeStamp
	maxTs := next.p.TimeStamp
	for {
		if next == nil {
			break
		}
		if minTs > next.p.TimeStamp {
			minTs = next.p.TimeStamp
		}
		if maxTs < next.p.TimeStamp {
			maxTs = next.p.TimeStamp
		}
		next = next.next
		chunkPackets += 1
	}

	var chunkDuration uint32 = 0
	c.streamedTimestamp, chunkDuration = c.outputReader.sendChunk(segment, c.streamedTimestamp)
	log.Infof("sendChunk %s: timestamp=%d, duration=%d, packets=%d, min/max=%d/%d", segment.sourceTag, c.streamedTimestamp, chunkDuration, chunkPackets, minTs, maxTs)

}

func (c *ChannelState) handleSlate(ts uint32) {
	stateChange := false
	if c.stream_state != source_slate {
		c.stream_state = source_slate
		c.slate.resetAsset(c.streamedTimestamp)
		stateChange = true
		log.Infof("handleSlate: stateChange=%t, ts=%d", stateChange, ts)
	}
	chk, err := c.slate.getChunk(ts)
	if err != nil {
		log.Errorf("unable to read slate chunk: %s", err)
	}
	if chk != nil {
		if stateChange {
			c.addChunk(chk, c.slate.currentSlate.metadataPacket)
		} else {
			c.addChunk(chk, nil)

		}
	}
	if c.slate.isFinal() {
		log.Infof("current chunk was final. aligning slate at new timestamp: %d", c.streamedTimestamp)
		c.slate.resetAsset(c.streamedTimestamp)
	}
}

func (c *ChannelState) handleStream(ts uint32) {
	chk, err := c.switcher.Read(c.streamedTimestamp)
	if err != nil {
		log.Errorf("switcher error: %s", err)
	}
	if chk != nil {
		stateChange := false
		if c.stream_state != source_stream {
			c.stream_state = source_stream
			stateChange = true
		}
		log.Infof("handleStream: stateChange=%t, ts=%d", stateChange, ts)

		//chk.resetTimestamp(c.streamedTimestamp)
		if stateChange {
			c.addChunk(chk, nil)
		} else {
			c.addChunk(chk, nil)
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
//func (c *ChannelState) selectSource() (string, *av.Packet, ChannelStream) {
//	if c.switcher.isReady() {
//		return "stream", c.switcher
//	}
//	if c.stream_state == stream_preroll {
//		slateSource := c.staticAssets.findByName(c.config.PrerollSlate)
//		return slateSource.Name, slateSource.metadataPacket, slateSource
//	}
//	return "", nil, nil
//}

// Generate time events for stream transit
// Work in chunk units
// each tick updates the timestamp and we try to Read chunks from a ChannelStream
// When read returns nil chunk we continue and wait for the next time event
// Add a timer main loop that generates an event with action=tick while
// updating c.timestamp
