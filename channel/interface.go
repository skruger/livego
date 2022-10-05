package channel

import "github.com/gwuhaolin/livego/av"

type ChannelStream interface {
	Align(timestamp uint32)
	Read(timestamp uint32) (*chunk, error)
}

// This interface only needs av.ReadCloser because the Info() method
// must return av.Info.Key="app/room"
type BeginOutputCallback func(reader av.ReadCloser) error
