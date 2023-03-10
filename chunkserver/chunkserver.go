package chunkserver

import (
	"context"
	"errors"
	"fmt"
	"log"

	pb "github.com/tyromancer/cdfs/pb"
)

type ChunkServer struct {
	pb.UnimplementedChunkServerServer

	// a mapping from ChunkHandle(string) to ChunkMetaData
	Chunks map[string]ChunkMetaData

	// a mapping from client token to client last sequence number
	ClientLastResp map[string]RespMetaData

	// globally unique server name
	ServerName string
}

// buildInvalidReadResp builds a pb.ReadResp that represents an invalid read
func buildInvalidReadResp(seqNum uint32, errorCode int32, errorMsg string) *pb.ReadResp {
	return &pb.ReadResp{SeqNum: seqNum, FileData: []byte{}, Status: &pb.Status{StatusCode: errorCode, ErrorMessage: errorMsg}}
}

// CreateChunk creates file on local filesystem that represents a chunk per Master Server's request
func (s *ChunkServer) CreateChunk(ctx context.Context, createChunkReq *pb.CreateChunkReq) (*pb.CreateChunkResp, error) {

	chunkHandle := createChunkReq.GetChunkHandle()

	// check if chunk already exists
	_, ok := s.Chunks[chunkHandle]
	if ok {
		errorCode := ERROR_CHUNK_ALREADY_EXISTS
		errorMsg := ErrorCodeToString(errorCode)
		res := &pb.CreateChunkResp{Status: &pb.Status{StatusCode: errorCode, ErrorMessage: errorMsg}}
		return res, errors.New(errorMsg)
	}

	// create file on disk
	chunkLocation := fmt.Sprintf("/cdfs/%s/%s", s.ServerName, chunkHandle)
	err := CreateFile(chunkLocation)
	if err != nil {
		errorCode := ERROR_CREATE_CHUNK_FAILED
		errorMsg := ErrorCodeToString(errorCode)
		res := &pb.CreateChunkResp{Status: &pb.Status{StatusCode: errorCode, ErrorMessage: errorMsg}}
		return res, err
	}

	// file create success, record metadata and return
	metadata := ChunkMetaData{ChunkLocation: chunkLocation, Role: Primary, PrimaryChunkServer: "", PeerAddress: createChunkReq.Peers, Used: 0}
	s.Chunks[chunkHandle] = metadata

	// TODO: send replicate request to peers

	return &pb.CreateChunkResp{Status: &pb.Status{StatusCode: OK, ErrorMessage: ErrorCodeToString(OK)}}, nil
}

// Read handles read request from client
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
	token := appendReq.Token
	seqNum := appendReq.SeqNum
	respMeta, ok := s.ClientLastResp[token]
	if ok {
		if seqNum == respMeta.LastSeq {
			return respMeta.AppendResp, respMeta.Err
		}
		if seqNum < respMeta.LastSeq {
			// should not happend
			errorCode := ERROR_APPEND_FAILED
			errorMsg := ErrorCodeToString(errorCode)
			res := &pb.AppendDataResp{Status: &pb.Status{StatusCode: errorCode, ErrorMessage: errorMsg}}
			return res, errors.New(errorMsg)
		}
	}

	chunkHandle := appendReq.ChunkHandle
	fileData := appendReq.FileData

	meta, ok := s.Chunks[chunkHandle]
	if ok {
		path := meta.ChunkLocation
		err := WriteFile(path, fileData)
		if err != nil {
			errorCode := ERROR_APPEND_FAILED
			errorMsg := ErrorCodeToString(errorCode)
			res := &pb.AppendDataResp{Status: &pb.Status{StatusCode: errorCode, ErrorMessage: errorMsg}}
			newResp := RespMetaData{LastSeq: seqNum, AppendResp: res, Err: err}
			s.ClientLastResp[token] = newResp
			return res, err
		}
		meta.Used += uint(len(fileData))

		//TODO: Notify Master the used length of chunk changed.

		res := &pb.AppendDataResp{Status: &pb.Status{StatusCode: OK, ErrorMessage: ErrorCodeToString(OK)}}
		newResp := RespMetaData{LastSeq: seqNum, AppendResp: res, Err: nil}
		s.ClientLastResp[token] = newResp
		return res, nil
	} else {
		errorCode := ERROR_APPEND_NOT_EXISTS
		errorMsg := ErrorCodeToString(errorCode)
		res := &pb.AppendDataResp{Status: &pb.Status{StatusCode: errorCode, ErrorMessage: errorMsg}}
		newResp := RespMetaData{LastSeq: seqNum, AppendResp: res, Err: errors.New(errorMsg)}
		s.ClientLastResp[token] = newResp
		return res, errors.New(errorMsg)
	}
}

func (s *ChunkServer) Replicate(ctx context.Context, replicateReq *pb.ReplicateReq) (*pb.ReplicateResp, error) {
	panic("ChunkServer.Replicate not implemented")
}
