package channel

import "github.com/gwuhaolin/livego/av"

type ChannelStream interface {
	Align(timestamp int)
	Read(timestamp int) (*chunk, error)
}

// This interface only needs av.ReadCloser because the Info() method
// must return av.Info.Key="app/room"
type BeginOutputCallback func(reader av.ReadCloser) error
