package chunkserver

import (
	"context"
	"errors"
	pb "github.com/tyromancer/cdfs/pb"
	"log"
)

type ChunkServer struct {
	pb.UnimplementedChunkServerServer

	// a mapping from ChunkHandle(string) to ChunkMetaData
	Chunks map[string]ChunkMetaData
}

// builds a pb.ReadResp that represents an invalid read
func buildInvalidReadResp(seqNum uint32, errorCode int32, errorMsg string) *pb.ReadResp {
	return &pb.ReadResp{SeqNum: seqNum, FileData: []byte{}, Status: &pb.Status{StatusCode: errorCode, ErrorMessage: errorMsg}}
}

// handles read request from client
func (s *ChunkServer) Read(ctx context.Context, readReq *pb.ReadReq) (*pb.ReadResp, error) {
	clientToken := readReq.Token

	log.Printf("Received read request from: %s\n", clientToken)

	requestedChunkHandle := readReq.ChunkHandle
	metadata, ok := s.Chunks[requestedChunkHandle]
	if ok && metadata.Role == Primary {
		chunkContent, err := LoadChunk(metadata.ChunkLocation)

		// if the read failed, return an invalid read response with error message
		if err != nil {
			log.Printf("Failed to read chunk at %s with error %v\n", chunkContent, err)
			errorCode := ERROR_READ_FAILED
			return buildInvalidReadResp(readReq.SeqNum, errorCode, ErrorCodeToString(errorCode)), err
		}

		// if the read was successful, return the chunk content with ok status
		return &pb.ReadResp{SeqNum: readReq.SeqNum, FileData: chunkContent, Status: &pb.Status{StatusCode: 0, ErrorMessage: ""}}, nil
	} else {
		// this chunk server either is not primary or does not have the requested chunk
		errorCode := ERROR_NOT_PRIMARY
		errorMessage := ErrorCodeToString(errorCode)
		return buildInvalidReadResp(readReq.SeqNum, errorCode, errorMessage), errors.New(errorMessage)
	}
}

func (s *ChunkServer) AppendData(ctx context.Context, appendReq *pb.AppendDataReq) (*pb.AppendDataResp, error) {
	panic("ChunkServer.AppendData not implemented")
}

func (s *ChunkServer) Replicate(ctx context.Context, replicateReq *pb.ReplicateReq) (*pb.ReplicateResp, error) {
	panic("ChunkServer.Replicate not implemented")
}
