package channel

import (
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
	streamTimestamp int
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

	loader := &chunkMaker{}
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
		Name:       config.Name,
		Filename:   config.Filename,
		firstChunk: loader.firstChunk,
	}

	return sa, nil
}

func (s *staticAsset) Align(timestamp int) {
	s.streamTimestamp = timestamp
	s.startTimestamp = s.firstChunk.startTimestamp
}

func (s *staticAsset) Read(timestamp int) (*chunk, error) {
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
