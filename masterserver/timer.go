package masterserver

import (
	"github.com/go-redis/redis/v8"
	"time"
)

type SnapshotTimer struct {
	DB       *redis.Client
	Interval int
	Srv      *MasterServer
}

func (t *SnapshotTimer) Snapshot() {
	for {
		select {
		case <-time.After(time.Duration(t.Interval) * time.Second):
			// TODO: take the snapshot

		}
	}
}
