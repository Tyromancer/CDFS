package chunkserver

import (
	"context"
	"github.com/tyromancer/cdfs/pb"
	"testing"
)

func TestClientReadInvalidRead(t *testing.T) {
	s := ChunkServer{Chunks: make(map[string]ChunkMetaData)}

	s.Chunks["chunk#1"] = ChunkMetaData{ChunkLocation: "/tmp/cs1/chunk1", Role: Secondary, PrimaryChunkServer: "cs2"}

	req1 := pb.ReadReq{SeqNum: 0, ChunkHandle: "chunk#2", Token: "client1"}
	got1, err := s.Read(context.Background(), &req1)
	wanted1 := ERROR_NOT_PRIMARY
	if got1.Status.GetStatusCode() != wanted1 || err == nil {
		t.Errorf("got status code %d, expected %d, returned error is %v", got1.Status.GetStatusCode(), wanted1, err)
	}
}
