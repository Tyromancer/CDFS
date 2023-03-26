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

type GetVersionTimer struct {
	Srv            *ChunkServer
	ChunkHandle    string
	Timeout        int
	PrimaryAddress string
	Quit           <-chan string
}

func (t *GetVersionTimer) Trigger() {
	for true {
		time.Sleep(time.Duration(t.Timeout) * time.Millisecond)
		//TODO: select
		//TODO: default: send GetVersionReq
	}
}
