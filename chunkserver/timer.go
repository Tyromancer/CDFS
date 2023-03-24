package chunkserver

import (
	"time"
)

type HeartBeatTimer struct {
	Srv *ChunkServer
	// Timeout in millisecond
	Timeout int
}

func (t *HeartBeatTimer) Trigger() {
	for true {
		time.Sleep(time.Duration(t.Timeout) * time.Millisecond)
		t.Srv.SendHeartBeat()
	}
}
