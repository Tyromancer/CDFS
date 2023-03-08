package chunkserver

import (
	"context"
	pb "github.com/tyromancer/cdfs/pb"
	"log"
)

type ChunkServer struct {
	pb.UnimplementedChunkServerServer

	// a mapping from ChunkID to ChunkMetaData
	Chunks map[ChunkHandle]ChunkMetaData
}

func (s *ChunkServer) Read(ctx context.Context, readReq *pb.ReadReq) (*pb.ReadResp, error) {
	clientToken := readReq.Token

	log.Printf("Received read request from: %s\n", clientToken)
	return &pb.ReadResp{SeqNum: readReq.SeqNum, FileData: []byte("Hello world")}, nil
}

func (s *ChunkServer) AppendData(ctx context.Context, appendReq *pb.AppendDataReq) (*pb.AppendDataResp, error) {
	panic("ChunkServer.AppendData not implemented")
}

func (s *ChunkServer) Replicate(ctx context.Context, replicateReq *pb.ReplicateReq) (*pb.ReplicateResp, error) {
	panic("ChunkServer.Replicate not implemented")
}
