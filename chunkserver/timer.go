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
	Srv         *ChunkServer
	ChunkHandle string
	Timeout     int
	Quit        <-chan string
}

func (t *GetVersionTimer) Trigger() {
	for true {
		time.Sleep(time.Duration(t.Timeout) * time.Millisecond)
		//TODO: select
		select {
		case <-t.Quit:
			return
		default:
			//TODO: default: send GetVersionReq

			t.Srv.SendGetVersion(t.ChunkHandle)
		}
	}
}
