package chunkserver

import (
	"context"
	"log"

	pb "github.com/tyromancer/cdfs/pb"
)

type ChunkServer struct {
	pb.UnimplementedChunkServerServer
}

func (s *ChunkServer) Read(ctx context.Context, readReq *pb.ReadReq) (*pb.ReadResp, error) {
	log.Printf("Received read request: %s\n", readReq)
	return &pb.ReadResp{SeqNum: readReq.SeqNum, FileData: []byte("Hello world")}, nil
}

func (s *ChunkServer) AppendData(ctx context.Context, appendReq *pb.AppendDataReq) (*pb.AppendDataResp, error) {
	panic("ChunkServer.AppendData not implemented")
}

func (s *ChunkServer) Replicate(ctx context.Context, replicateReq *pb.ReplicateReq) (*pb.ReplicateResp, error) {
	panic("ChunkServer.Replicate not implemented")
}
