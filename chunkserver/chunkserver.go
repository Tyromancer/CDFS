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

// CreateChunk creates file on local filesystem that represents a chunk per Master Server's request
func (s *ChunkServer) CreateChunk(ctx context.Context, createChunkReq *pb.CreateChunkReq) (*pb.CreateChunkResp, error) {

	chunkHandle := createChunkReq.GetChunkHandle()

	// check if chunk already exists
	_, ok := s.Chunks[chunkHandle]
	if ok {
		res := NewCreateChunkResp(ERROR_CREATE_CHUNK_FAILED)
		return res, errors.New(res.GetStatus().ErrorMessage)
	}

	// create file on disk
	chunkLocation := fmt.Sprintf("/cdfs/%s/%s", s.ServerName, chunkHandle)
	err := CreateFile(chunkLocation)
	if err != nil {
		res := NewCreateChunkResp(ERROR_CREATE_CHUNK_FAILED)
		return res, err
	}

	// file create success, record metadata and return
	metadata := ChunkMetaData{ChunkLocation: chunkLocation, Role: Primary, PrimaryChunkServer: "", PeerAddress: createChunkReq.Peers, Used: 0}
	s.Chunks[chunkHandle] = metadata

	// TODO: send replicate request to peers

	return NewCreateChunkResp(OK), nil
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
			return NewReadResp(readReq.SeqNum, nil, errorCode), err
		}

		// if the read was successful, return the chunk content with ok status
		return NewReadResp(readReq.SeqNum, chunkContent, OK), nil
	} else {
		// this chunk server either is not primary or does not have the requested chunk
		res := NewReadResp(readReq.SeqNum, nil, ERROR_NOT_PRIMARY)
		return res, errors.New(res.GetStatus().ErrorMessage)
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
			// should not happen
			res := NewAppendDataResp(ERROR_APPEND_FAILED)
			return res, errors.New(res.GetStatus().ErrorMessage)
		}
	}

	chunkHandle := appendReq.ChunkHandle
	fileData := appendReq.FileData

	meta, ok := s.Chunks[chunkHandle]
	if ok {
		path := meta.ChunkLocation
		err := WriteFile(path, fileData)
		if err != nil {
			res := NewAppendDataResp(ERROR_APPEND_FAILED)
			newResp := RespMetaData{LastSeq: seqNum, AppendResp: res, Err: err}
			s.ClientLastResp[token] = newResp
			return res, err
		}
		meta.Used += uint(len(fileData))

		//TODO: Notify Master the used length of chunk changed.

		res := NewAppendDataResp(OK)
		newResp := RespMetaData{LastSeq: seqNum, AppendResp: res, Err: nil}
		s.ClientLastResp[token] = newResp
		return res, nil
	} else {
		res := NewAppendDataResp(ERROR_APPEND_NOT_EXISTS)
		newResp := RespMetaData{LastSeq: seqNum, AppendResp: res, Err: errors.New(res.Status.ErrorMessage)}
		s.ClientLastResp[token] = newResp
		return res, errors.New(res.GetStatus().ErrorMessage)
	}
}

func (s *ChunkServer) Replicate(ctx context.Context, replicateReq *pb.ReplicateReq) (*pb.ReplicateResp, error) {
	panic("ChunkServer.Replicate not implemented")
}
