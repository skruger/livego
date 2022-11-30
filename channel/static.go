package channel

import (
	"fmt"
	"github.com/gwuhaolin/livego/av"
	"github.com/gwuhaolin/livego/configure"
	"github.com/gwuhaolin/livego/container/flv"
	log "github.com/sirupsen/logrus"
	"os"
	"time"
)

type staticAsset struct {
	Name     string
	Filename string
	//FileReader *flv.FLVReader
	firstChunk      *chunk
	currentChunk    *chunk
	startTimestamp  uint32
	streamTimestamp uint32
	metadataPacket  *av.Packet
}

func newStaticAsset(config configure.SlateConfig) (*staticAsset, error) {
	defer func() {
		if r := recover(); r != nil {
			log.Errorf("slate (%s) loader panic: %s", config.Filename, r)
			time.Sleep(1 * time.Second)
		}
	}()

	file, err := os.Open(config.Filename)
	if err != nil {
		return nil, err
	}

	flvReader, err := flv.NewFLVReader(file)
	if err != nil {
		return nil, err
	}

	var recentMetadataPacket *av.Packet

	loader := &chunkMaker{
		metadataCb: func(p *av.Packet) {
			recentMetadataPacket = p
		},
		sourceTag: fmt.Sprintf("slate/%s", config.Name),
	}
	packetCount, err := loader.loadSlate(flvReader)
	if err != nil {
		log.Warning("loadSlate encountered an error: ", err)
	}

	err = file.Close()
	if err != nil {
		log.Warning("problem closing file after loading static asset: ", err)
	}

	log.Infof("Loaded slate '%s' from %s in %d chunks (%d packets)", config.Name, config.Filename, loader.chunkCount, packetCount)
	sa := &staticAsset{
		Name:           config.Name,
		Filename:       config.Filename,
		firstChunk:     loader.firstChunk,
		metadataPacket: recentMetadataPacket,
	}

	return sa, nil
}

func (s *staticAsset) Align(timestamp uint32) {
	s.streamTimestamp = timestamp
	s.startTimestamp = s.firstChunk.startTimestamp
}

func (s *staticAsset) Read(timestamp uint32) (*chunk, error) {
	if s.currentChunk == nil {
		s.currentChunk = s.firstChunk
	}
	streamProgress := timestamp - s.streamTimestamp
	maxChunkTimestamp := s.startTimestamp + uint32(streamProgress)
	if maxChunkTimestamp > s.currentChunk.startTimestamp {
		thisChunk := s.currentChunk
		s.currentChunk = thisChunk.next
		return thisChunk, nil
	}
	return nil, nil
}

type staticAssets []*staticAsset

func (s *staticAssets) findByName(name string) *staticAsset {
	for _, source := range *s {
		if source.Name == name {
			return source
		}
	}
	return nil
}

type slateProvider struct {
	staticAssets staticAssets
	currentSlate *staticAsset
}

func newSlateProvider() *slateProvider {
	return &slateProvider{}
}

func (s *slateProvider) addSlate(asset *staticAsset) {
	s.staticAssets = append(s.staticAssets, asset)
	if s.currentSlate == nil {
		s.currentSlate = asset
	}
}

func (s *slateProvider) setSlate(name string) error {
	slate := s.staticAssets.findByName(name)
	if slate != nil {
		s.currentSlate = slate
		return nil
	}
	return fmt.Errorf("slate not found: %s", name)
}

func (s *slateProvider) isFinal() bool {
	if s.currentSlate == nil || s.currentSlate.currentChunk == nil {
		return false
	}
	return s.currentSlate.currentChunk.isFinal()
}

func (s *slateProvider) resetAsset(ts uint32) {
	if s.currentSlate == nil {
		return
	}
	s.currentSlate.currentChunk = s.currentSlate.firstChunk
	s.currentSlate.Align(ts)
}

func (s *slateProvider) getChunk(ts uint32) (*chunk, error) {
	return s.currentSlate.Read(ts)
}
