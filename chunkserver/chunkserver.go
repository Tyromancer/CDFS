package chunkserver

import (
	"context"
	"errors"
	"fmt"
	pb "github.com/tyromancer/cdfs/pb"
	"google.golang.org/grpc"
	"log"
	"path"
)

type ChunkServer struct {
	pb.UnimplementedChunkServerServer

	// a mapping from ChunkHandle(string) to ChunkMetaData
	Chunks map[string]*ChunkMetaData

	// a mapping from client token to client last sequence number
	ClientLastResp map[string]RespMetaData

	// globally unique server name
	ServerName string

	// base directory to store chunk files
	BasePath string

	HostName string

	Port uint32
}

func (s *ChunkServer) SendRegister(masterIP string, masterPort uint32) error {
	csRegisterReq := pb.CSRegisterReq{
		Host: s.HostName,
		Port: s.Port,
	}
	var conn *grpc.ClientConn

	conn, err := grpc.Dial(fmt.Sprintf("%s:%d", masterIP, masterPort), grpc.WithInsecure())

	if err != nil {
		log.Fatalf("Failed to connect to Master %s:%d", masterIP, masterPort)
	}
	defer conn.Close()

	c := pb.NewMasterClient(conn)
	res, err := c.CSRegister(context.Background(), &csRegisterReq)

	if err != nil || res.GetStatus().GetStatusCode() != OK {
		log.Fatalf("Error send CSRegister Request: %v", err)
	}

	return nil
}

// CreateChunk creates file on local filesystem that represents a chunk per Master Server's request
func (s *ChunkServer) CreateChunk(ctx context.Context, createChunkReq *pb.CreateChunkReq) (*pb.CreateChunkResp, error) {

	chunkHandle := createChunkReq.GetChunkHandle()

	// check if chunk already exists
	_, ok := s.Chunks[chunkHandle]
	if ok {
		res := NewCreateChunkResp(ERROR_CHUNK_ALREADY_EXISTS)
		return res, errors.New(res.GetStatus().ErrorMessage)
	}

	// file create success, record metadata and return
	primaryChunkServer := ""
	if createChunkReq.GetRole() != Primary {
		primaryChunkServer = createChunkReq.Primary
	}

	// send replicate request to peers
	// TODO: For Master: when receive error, send deleteCreatedChunk message to all
	if createChunkReq.GetRole() == Primary {
		for _, peer := range createChunkReq.GetPeers() {
			forwardErr := ForwardCreateReq(createChunkReq, peer)
			if forwardErr != nil {
				// abort create process and return error message
				res := NewCreateChunkResp(ERROR_CREATE_CHUNK_FAILED)
				return res, forwardErr
			}
		}
	}

	// create file on disk
	// chunkLocation := fmt.Sprintf("/cdfs/%s/%s", s.ServerName, chunkHandle)
	chunkLocation := path.Join(s.BasePath, chunkHandle)
	err := CreateFile(chunkLocation)
	if err != nil {
		res := NewCreateChunkResp(ERROR_CREATE_CHUNK_FAILED)
		return res, err
	}

	metadata := ChunkMetaData{ChunkLocation: chunkLocation, Role: createChunkReq.GetRole(), PrimaryChunkServer: primaryChunkServer, PeerAddress: createChunkReq.Peers, Used: 0, Version: 0}
	s.Chunks[chunkHandle] = &metadata

	return NewCreateChunkResp(OK), nil
}

// Read handles read request from client
func (s *ChunkServer) Read(ctx context.Context, readReq *pb.ReadReq) (*pb.ReadResp, error) {
	clientToken := readReq.Token
	//TODO: add version in read response
	log.Printf("Received read request from: %s\n", clientToken)

	requestedChunkHandle := readReq.ChunkHandle
	metadata, ok := s.Chunks[requestedChunkHandle]
	if ok && metadata.Role == Primary {
		chunkContent, err := LoadChunk(metadata.ChunkLocation)

		// if the read failed, return an invalid read response with error message
		if err != nil {
			log.Printf("Failed to read chunk at %s with error %v\n", chunkContent, err)
			errorCode := ERROR_READ_FAILED
			return NewReadResp(nil, errorCode), err
		}

		// if the read was successful, return the chunk content with ok status
		return NewReadResp(chunkContent, OK), nil
	} else {
		// this chunk server either is not primary or does not have the requested chunk
		res := NewReadResp(nil, ERROR_NOT_PRIMARY)
		return res, errors.New(res.GetStatus().ErrorMessage)
	}
}

func (s *ChunkServer) AppendData(ctx context.Context, appendReq *pb.AppendDataReq) (*pb.AppendDataResp, error) {
	token := appendReq.GetToken()
	newID := appendReq.GetUuid()
	chunkHandle := appendReq.GetChunkHandle()
	chunkMeta, ok := s.Chunks[chunkHandle]
	if ok {
		role := chunkMeta.Role
		if role != Primary {
			res := NewAppendDataResp(ERROR_NOT_PRIMARY)
			return res, errors.New(res.GetStatus().ErrorMessage)
		}
	} else {
		//if chunk not exist
		res := NewAppendDataResp(ERROR_APPEND_NOT_EXISTS)
		//newResp := RespMetaData{LastID: newID, AppendResp: res, Err: errors.New(res.Status.ErrorMessage)}
		//s.ClientLastResp[token] = newResp
		return res, errors.New(res.GetStatus().ErrorMessage)
	}

	// chunk exist and current chunk server is the primary of target chunk handle
	respMeta, ok := s.ClientLastResp[token]
	//If client already executed with the server before
	if ok {
		lastID := respMeta.LastID
		if lastID == newID {
			lastResp := respMeta.AppendResp
			if lastResp.Status.GetStatusCode() == OK {
				return lastResp, nil
			}
		}
	}

	fileData := appendReq.FileData
	curVersion := chunkMeta.Version

	//TODO: ReplicateReq to peers, wait for ACKS
	replicateReq := &pb.ReplicateReq{
		ClientToken: token, ChunkHandle: chunkHandle, FileData: fileData, ReqID: newID, Version: curVersion + 1}
	for _, peer := range chunkMeta.PeerAddress {
		sendErr := NewReplicateReq(replicateReq, peer)
		if sendErr != nil {
			//abort send replicate process and return error message
			res := NewAppendDataResp(ERROR_APPEND_FAILED)
			return res, sendErr
		}
	}

	err := WriteFile(chunkMeta, fileData)
	if err != nil {
		res := NewAppendDataResp(ERROR_APPEND_FAILED)
		newResp := RespMetaData{LastID: newID, AppendResp: res, Err: err}
		s.ClientLastResp[token] = newResp
		return res, err
	}

	res := NewAppendDataResp(OK)
	newResp := RespMetaData{LastID: newID, AppendResp: res, Err: nil}
	s.ClientLastResp[token] = newResp
	return res, nil
}

func (s *ChunkServer) Replicate(ctx context.Context, replicateReq *pb.ReplicateReq) (*pb.ReplicateResp, error) {
	clientToken := replicateReq.GetClientToken()
	dataVersionNumber := replicateReq.GetVersion()
	chunkHandle := replicateReq.GetChunkHandle()
	requestUUID := replicateReq.GetReqID()

	currentChunkMeta, ok := s.Chunks[chunkHandle]

	if !ok {
		// TODO: chunk not exist on server, return error message
		res := NewReplicateResp(ERROR_REPLICATE_NOT_EXISTS, requestUUID)
		return res, errors.New(res.GetStatus().GetErrorMessage())
	}

	// chunk exists on this server, check role
	currentRole := currentChunkMeta.Role
	if currentRole != Secondary {
		res := NewReplicateResp(ERROR_NOT_SECONDARY, requestUUID)
		return res, errors.New(res.GetStatus().GetErrorMessage())
	}

	// role is secondary (backup)
	// TODO: check version number
	currentVersionNumber := currentChunkMeta.Version
	if currentVersionNumber < dataVersionNumber-1 { // need fetch from primary
		// TODO: fetch from master (directly here or new RPC request from primary?)
	} else if currentVersionNumber == dataVersionNumber-1 { // apply append
		// TODO: append data to disk
		chunkContent := replicateReq.GetFileData()
		err := WriteFile(currentChunkMeta, chunkContent)

		if err != nil { // write failed
			res := NewReplicateResp(ERROR_REPLICATE_FAILED, requestUUID)
			return res, err
		}

		res := NewReplicateResp(OK, requestUUID)
		// add response in server's cached client last response
		resMeta := RespMetaData{
			LastID:     requestUUID,
			AppendResp: ReplicateRespToAppendResp(res),
			Err:        nil,
		}
		s.ClientLastResp[clientToken] = resMeta
		return res, nil
	}

	// return error
	res := NewReplicateResp(ERROR_SHOULD_NOT_HAPPEN, requestUUID)
	return res, errors.New(res.GetStatus().GetErrorMessage())
}
