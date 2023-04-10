package chunkserver

import (
	"context"
	"fmt"
	"log"
	"os"
	"path"
	"sync"

	pb "github.com/tyromancer/cdfs/pb"
	"google.golang.org/grpc"
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

	MasterIP string

	MasterPort uint32

	Debug     bool
	DebugChan chan DebugInfo
}

func (s *ChunkServer) SendRegister() error {
	csRegisterReq := pb.CSRegisterReq{
		Host: s.HostName,
		Port: s.Port,
	}
	var conn *grpc.ClientConn

	conn, err := grpc.Dial(fmt.Sprintf("%s:%d", s.MasterIP, s.MasterPort), grpc.WithInsecure())

	if err != nil {
		log.Fatalf("Failed to connect to Master %s:%d", s.MasterIP, s.MasterPort)
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
		//return res, errors.New(res.GetStatus().ErrorMessage)
		return res, nil
	}

	// send replicate request to peers
	// For Master: when receive error, send deleteCreatedChunk message to all
	if createChunkReq.GetRole() == Primary {
		for _, peer := range createChunkReq.GetPeers() {
			forwardErr := ForwardCreateReq(createChunkReq, peer)
			if forwardErr != nil {
				// abort create process and return error message
				res := NewCreateChunkResp(ERROR_CREATE_CHUNK_FAILED)
				return res, nil
				//return res, forwardErr
			}
		}
	}

	// create file on disk
	// chunkLocation := fmt.Sprintf("/cdfs/%s/%s", s.ServerName, chunkHandle)
	// chunkLocation := path.Join(s.BasePath, chunkHandle)
	chunkLocation := s.BasePath + "/" + chunkHandle
	err := CreateFile(chunkLocation)
	if err != nil {
		res := NewCreateChunkResp(ERROR_CREATE_CHUNK_FAILED)
		return res, nil
		//return res, err
	}

	metadata := ChunkMetaData{ChunkLocation: chunkLocation, Role: createChunkReq.GetRole(), PrimaryChunkServer: "", PeerAddress: createChunkReq.Peers, Used: 0, Version: 0, GetVersionChannel: nil}
	s.Chunks[chunkHandle] = &metadata

	return NewCreateChunkResp(OK), nil
}

// ForwardCreate create new chunk as backup
func (s *ChunkServer) ForwardCreate(ctx context.Context, forwardCreateReq *pb.ForwardCreateReq) (*pb.ForwardCreateResp, error) {
	chunkHandle := forwardCreateReq.GetChunkHandle()

	// check if chunk already exists
	_, ok := s.Chunks[chunkHandle]
	if ok {
		res := NewForwardCreateResp(ERROR_CHUNK_ALREADY_EXISTS)
		return res, nil
		//return res, errors.New(res.GetStatus().ErrorMessage)
	}

	err := s.createChunkFile(chunkHandle, forwardCreateReq.GetPrimary())
	if err != nil {
		res := NewForwardCreateResp(ERROR_CREATE_CHUNK_FAILED)
		return res, nil
	}
	return NewForwardCreateResp(OK), nil

}

// DeleteChunk deletes the chunk metadata that corresponds with a string chunk handle on the primary
// chunk server as well as on backup servers
func (s *ChunkServer) DeleteChunk(ctx context.Context, deleteReq *pb.DeleteChunkReq) (*pb.DeleteChunkResp, error) {
	// check if chunk metadata exists in metadata
	chunkHandle := deleteReq.GetChunkHandle()
	metaData, ok := s.Chunks[chunkHandle]
	if ok {
		if metaData.GetVersionChannel != nil && !IsClose(metaData.GetVersionChannel) {
			close(metaData.GetVersionChannel)
			metaData.GetVersionChannel = nil
		}
		delete(s.Chunks, chunkHandle)
	}

	// NOTE: will delete always return OK?
	res := NewDeleteChunkResp(OK)
	return res, nil
}

// ReadVersion returns the version of a chunk that corresponds with a string chunk handle and returns error if the chunk
// is not recorded by the ChunkServer
func (s *ChunkServer) ReadVersion(ctx context.Context, readVersion *pb.ReadVersionReq) (*pb.ReadVersionResp, error) {
	chunkHandle := readVersion.GetChunkHandle()
	meta, ok := s.Chunks[chunkHandle]
	if !ok {
		res := NewReadVersionResp(ERROR_CHUNK_NOT_EXISTS, nil)
		return res, nil
		//return res, errors.New(res.GetStatus().GetErrorMessage())
	}

	versionNum := meta.Version
	res := NewReadVersionResp(OK, &versionNum)
	return res, nil
}

// Read handles read request from client
func (s *ChunkServer) Read(ctx context.Context, readReq *pb.ReadReq) (*pb.ReadResp, error) {
	clientToken := readReq.Token
	// add version in read response
	log.Printf("Received read request from: %s\n", clientToken)

	requestedChunkHandle := readReq.ChunkHandle
	readStart := readReq.GetStart()
	readEnd := readReq.GetEnd()
	metadata, ok := s.Chunks[requestedChunkHandle]

	if ok {
		chunkContent, err := LoadChunk(metadata.ChunkLocation, metadata.Used, readStart, readEnd)

		// if the read failed, return an invalid read response with error message and nil version number
		if err != nil {
			log.Printf("Failed to read chunk at %s with error %v\n", chunkContent, err)
			errorCode := ERROR_READ_FAILED
			return NewReadResp(nil, errorCode, nil), nil
			//return NewReadResp(nil, errorCode, nil), err
		}

		// if the read was successful, return the chunk content with ok status and nil version number
		chunkVersion := metadata.Version
		return NewReadResp(chunkContent, OK, &chunkVersion), nil
	} else {
		// this chunk server either is not primary or does not have the requested chunk
		res := NewReadResp(nil, ERROR_READ_FAILED, nil)
		return res, nil
		//return res, errors.New(res.GetStatus().ErrorMessage)
	}
}

func (s *ChunkServer) AppendData(ctx context.Context, appendReq *pb.AppendDataReq) (*pb.AppendDataResp, error) {
	token := appendReq.GetToken()
	newID := appendReq.GetUuid()
	chunkHandle := appendReq.GetChunkHandle()
	chunkMeta, ok := s.Chunks[chunkHandle]
	appendSize := uint32(len(appendReq.FileData))
	if ok {
		role := chunkMeta.Role
		if role != Primary {
			res := NewAppendDataResp(ERROR_NOT_PRIMARY)
			err := sendAppendResult(chunkHandle, appendSize, res.GetStatus(), s.MasterIP, s.MasterPort, s.Debug)
			if err != nil {
				log.Println("Send Append Result to Master: ", err)
			}
			return res, nil
			//return res, errors.New(res.GetStatus().ErrorMessage)
		}
	} else {
		//if chunk not exist
		res := NewAppendDataResp(ERROR_APPEND_NOT_EXISTS)
		//res := &pb.AppendDataResp{Status: &pb.Status{StatusCode: ERROR_APPEND_NOT_EXISTS, ErrorMessage: ErrorCodeToString(ERROR_APPEND_NOT_EXISTS)}}

		//newResp := RespMetaData{LastID: newID, AppendResp: res, Err: errors.New(res.Status.ErrorMessage)}
		//s.ClientLastResp[token] = newResp
		err := sendAppendResult(chunkHandle, appendSize, res.GetStatus(), s.MasterIP, s.MasterPort, s.Debug)
		if err != nil {
			log.Println("Send Append Result to Master: ", err)
		}

		return res, nil
		//return res, status.Errorf(codes.NotFound, "append not exists")
		//return res, errors.New(res.GetStatus().ErrorMessage)
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

	// ReplicateReq to peers, wait for ACKS
	// rewrite replicate requests in concurrent manner
	wg := sync.WaitGroup{}
	wg.Add(len(chunkMeta.PeerAddress))
	replicateErrors := make([]int, len(chunkMeta.PeerAddress))
	replicateReq := &pb.ReplicateReq{
		ClientToken: token, ChunkHandle: chunkHandle, FileData: fileData, ReqID: newID, Version: curVersion + 1}

	for i, peer := range chunkMeta.PeerAddress {
		go func(idx int, peerAddr string) {
			sendErr := NewReplicateReq(replicateReq, peerAddr)
			if sendErr != nil {
				replicateErrors[idx] = 1
			} else {
				replicateErrors[idx] = 0
			}
			wg.Done()
		}(i, peer)
	}
	wg.Wait()
	errorCount := Sum(replicateErrors)
	if errorCount > len(chunkMeta.PeerAddress)/2 {
		res := NewAppendDataResp(ERROR_APPEND_FAILED)
		err := sendAppendResult(chunkHandle, appendSize, res.GetStatus(), s.MasterIP, s.MasterPort, s.Debug)
		if err != nil {
			log.Println("Send Append Result to Master: ", err)
		}
		return res, nil
		//return res, errors.New(res.GetStatus().GetErrorMessage())
	}

	err := WriteFile(chunkMeta, fileData)
	if err != nil {
		panic("failed to write to disk")

		//res := NewAppendDataResp(ERROR_APPEND_FAILED)
		//newResp := RespMetaData{LastID: newID, AppendResp: res, Err: err}
		//s.ClientLastResp[token] = newResp
		//masterSendErr := sendAppendResult(chunkHandle, token, res.GetStatus(), s.MasterIP, s.MasterPort)
		//if masterSendErr != nil {
		//	log.Println("Send Append Result to Master: ", masterSendErr)
		//}
		//return res, err
	}

	res := NewAppendDataResp(OK)
	newResp := RespMetaData{LastID: newID, AppendResp: res, Err: nil}
	s.ClientLastResp[token] = newResp
	masterSendErr := sendAppendResult(chunkHandle, appendSize, res.GetStatus(), s.MasterIP, s.MasterPort, s.Debug)
	if masterSendErr != nil {
		log.Println("Send Append Result to Master: ", masterSendErr)
	}
	return res, nil
}

func sendAppendResult(chunkHandle string, size uint32, status *pb.Status, masterIP string, masterPort uint32, debug bool) error {
	if debug && (masterIP == "" && masterPort == 0) {
		return nil
	}
	peerConn, err := NewPeerConn(fmt.Sprintf("%s:%d", masterIP, masterPort))
	if err != nil {
		return err
	}
	defer peerConn.Close()
	peerClient := pb.NewMasterClient(peerConn)
	appendResult := &pb.AppendResultReq{
		ChunkHandle: chunkHandle,
		Size:        size,
		Status:      status,
	}
	_, err = peerClient.AppendResult(context.Background(), appendResult)
	return err
}

func (s *ChunkServer) Replicate(ctx context.Context, replicateReq *pb.ReplicateReq) (*pb.ReplicateResp, error) {
	clientToken := replicateReq.GetClientToken()
	dataVersionNumber := replicateReq.GetVersion()
	chunkHandle := replicateReq.GetChunkHandle()
	requestUUID := replicateReq.GetReqID()

	currentChunkMeta, ok := s.Chunks[chunkHandle]

	if !ok {
		// chunk not exist on server, return error message
		res := NewReplicateResp(ERROR_REPLICATE_NOT_EXISTS, requestUUID)
		return res, nil
		//return res, errors.New(res.GetStatus().GetErrorMessage())
	}

	// chunk exists on this server, check role
	currentRole := currentChunkMeta.Role
	if currentRole != Secondary {
		res := NewReplicateResp(ERROR_NOT_SECONDARY, requestUUID)
		return res, nil
		//return res, errors.New(res.GetStatus().GetErrorMessage())
	}

	// role is secondary (backup)
	// check version number
	currentVersionNumber := currentChunkMeta.Version
	if currentVersionNumber < dataVersionNumber-1 { // need fetch from primary
		// return error (hopefully timer will fetch the latest data from primary)
		res := NewReplicateResp(ERROR_VERSIONS_DO_NOT_MATCH, requestUUID)
		return res, nil
		//return res, errors.New(res.GetStatus().GetErrorMessage())
	} else if currentVersionNumber == dataVersionNumber-1 { // apply append
		// append data to disk
		chunkContent := replicateReq.GetFileData()
		err := WriteFile(currentChunkMeta, chunkContent)

		if err != nil { // write failed
			panic("failed to write to disk")
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
	return res, nil
	//return res, errors.New(res.GetStatus().GetErrorMessage())
}

func (s *ChunkServer) GetVersion(ctx context.Context, req *pb.GetVersionReq) (res *pb.GetVersionResp, err error) {
	chunkHandle := req.GetChunkHandle()
	version := req.GetVersion()

	meta, ok := s.Chunks[chunkHandle]
	defer func() {
		if s.Debug == true {
			debugInfo := DebugInfo{
				Addr:       s.ServerName,
				Func:       "GetVersion",
				StatusCode: res.GetStatus().GetStatusCode(),
			}
			if s.DebugChan != nil && !IsClose(s.DebugChan) {
				s.DebugChan <- debugInfo
			}
		}
	}()
	if !ok {
		// indicate chunk was deleted
		res := NewGetVersionResp(ERROR_CHUNK_NOT_EXISTS, nil, nil)
		return res, nil
		//return res, errors.New(res.GetStatus().GetErrorMessage())
	}

	meta.MetaDataLock.Lock()
	defer meta.MetaDataLock.Unlock()

	// check role
	role := meta.Role
	if role != Primary {
		res := NewGetVersionResp(ERROR_NOT_PRIMARY, nil, nil)
		return res, nil
		//return res, errors.New(res.GetStatus().GetErrorMessage())
	}

	// check version
	currentVersion := meta.Version
	if currentVersion == version {
		res := NewGetVersionResp(OK, &currentVersion, nil)
		return res, nil
	}

	// versions don't match: send file content back to caller
	// fetch real chunk data and set fileData field
	chunkData, err := LoadChunk(meta.ChunkLocation, meta.Used, 0, 0)
	if err != nil {
		res := NewGetVersionResp(ERROR_READ_FAILED, nil, nil)
		return res, nil
		//return res, err

	}
	return NewGetVersionResp(ERROR_VERSIONS_DO_NOT_MATCH, &currentVersion, chunkData), nil

	//return NewGetVersionResp(ERROR_VERSIONS_DO_NOT_MATCH, &currentVersion, chunkData), errors.New(ErrorCodeToString(ERROR_VERSIONS_DO_NOT_MATCH))
}

func (s *ChunkServer) SendHeartBeat() {

	chunkLen := len(s.Chunks)
	chunkHandles := make([]string, chunkLen)
	usedSizes := make([]uint32, chunkLen)
	i := 0
	for chunkHandle, metaData := range s.Chunks {
		//chunkHandles = append(chunkHandles, chunkHandle)
		//usedSizes = append(usedSizes, metaData.Used)
		chunkHandles[i] = chunkHandle
		usedSizes[i] = metaData.Used
		i++
	}
	newHeartBeat := pb.HeartBeatPayload{
		ChunkHandle: chunkHandles,
		Used:        usedSizes,
		Name:        s.ServerName,
	}

	peerConn, err := NewPeerConn(fmt.Sprintf("%s:%d", s.MasterIP, s.MasterPort))
	defer peerConn.Close()
	if err != nil {
		log.Println("Cannot connect to Master: ", err)
		return
	}

	peerClient := pb.NewMasterClient(peerConn)
	res, err := peerClient.HeartBeat(context.Background(), &newHeartBeat)
	// Check special flag for dead status
	if err != nil { // connection error
		log.Println("HeartBeat connection error: ", err)
	}

	// TODO: decide on which error code to use for this case
	if res.GetStatus().GetStatusCode() != OK {
		// remove all chunk meta from memory
		for chunkHandle, meta := range s.Chunks {
			meta.MetaDataLock.Lock()
			defer meta.MetaDataLock.Unlock()
			delete(s.Chunks, chunkHandle)
		}

		for clientToken, _ := range s.ClientLastResp {
			delete(s.ClientLastResp, clientToken)
		}
		// remove all chunk file on disk
		err = os.RemoveAll(s.BasePath)
		if err != nil {
			log.Printf("failed to remove file from disk: %v", err)
		}
	}
	return
}

func (s *ChunkServer) SendGetVersion(chunkHandle string) {

	meta, ok := s.Chunks[chunkHandle]
	if !ok {
		log.Println("SendGetVersion: chunk was deleted")
		return
	}
	primaryAddress := meta.PrimaryChunkServer
	conn, err := NewPeerConn(primaryAddress)
	if err != nil {
		log.Println("SendGetVersion: failed to connect to primary at ", primaryAddress)
		return
	}
	primary := pb.NewChunkServerClient(conn)
	req := &pb.GetVersionReq{
		ChunkHandle: chunkHandle,
		Version:     meta.Version,
	}

	res, err := primary.GetVersion(context.Background(), req)
	resStatusCode := res.GetStatus().GetStatusCode()
	if err != nil || (resStatusCode != ERROR_VERSIONS_DO_NOT_MATCH && resStatusCode != OK) {
		log.Printf("Received error for get version: %s", res.GetStatus().GetErrorMessage())
	}

	// check status code
	statusCode := res.GetStatus().GetStatusCode()
	switch statusCode {
	case OK:
		return
	case ERROR_CHUNK_NOT_EXISTS:
		log.Printf("SendGetVersion: chunk does not exist on primary")
		return
	case ERROR_NOT_PRIMARY:
		log.Printf("SendGetVersion: peer is no longer primary")
		return
	case ERROR_READ_FAILED:
		log.Printf("SendGetVersion: read failed on primary")
		return
	case ERROR_VERSIONS_DO_NOT_MATCH:
		// overwrite received chunk data onto disk
		meta.MetaDataLock.Lock()
		defer meta.MetaDataLock.Unlock()

		// check version number
		if meta.Version == res.GetVersion() {
			return
		}

		err = OverWriteChunk(meta, res.GetChunkData())
		if err != nil {
			log.Printf("SendGetVersion: failed to overwrite chunk %s with error %v", chunkHandle, err)
		}
	default:
		log.Println("SendGetVersion: should not get here")
	}
}

// Fault Tolerance Functions

// ChangeToPrimary will receive by backup chunk server, to notice it be the new Primary with certain chunk handle.
func (s *ChunkServer) ChangeToPrimary(ctx context.Context, req *pb.ChangeToPrimaryReq) (res *pb.ChangeToPrimaryResp, err error) {
	chunkHandle := req.GetChunkHandle()
	//newRole := req.GetRole()
	newPeers := req.GetPeers()

	// check if the chunkhandle exist in the chunkserver
	meta, ok := s.Chunks[chunkHandle]
	if !ok {
		res := &pb.ChangeToPrimaryResp{
			Status: NewStatus(ERROR_CHUNK_NOT_EXISTS),
		}
		return res, nil
	}
	// Check if the chunk server is the primary
	role := meta.Role
	if role == Primary {
		res := &pb.ChangeToPrimaryResp{
			Status: NewStatus(ERROR_NOT_SECONDARY),
		}
		return res, nil
	}
	// Update the metadata
	meta.MetaDataLock.Lock()
	defer meta.MetaDataLock.Unlock()
	meta.Role = Primary
	meta.PeerAddress = newPeers
	meta.PrimaryChunkServer = ""
	if meta.GetVersionChannel != nil && !IsClose(meta.GetVersionChannel) {
		close(meta.GetVersionChannel)
		meta.GetVersionChannel = nil
	}
	res = &pb.ChangeToPrimaryResp{
		Status: NewStatus(OK),
	}
	return res, nil
}

// AssignNewPrimary role to certain chunkhandle change the primary.
func (s *ChunkServer) AssignNewPrimary(ctx context.Context, req *pb.AssignNewPrimaryReq) (res *pb.AssignNewPrimaryResp, err error) {
	// Check Role, if chunk handle exists
	chunkHandle := req.GetChunkHandle()
	newPrimary := req.GetPrimary()
	meta, ok := s.Chunks[chunkHandle]
	if ok {
		role := meta.Role
		if role == Primary {
			res := &pb.AssignNewPrimaryResp{
				Status: NewStatus(ERROR_NOT_SECONDARY),
			}
			return res, nil
		}
		// chunk server is backup
		meta.MetaDataLock.Lock()
		defer meta.MetaDataLock.Unlock()
		meta.PrimaryChunkServer = newPrimary
		res := &pb.AssignNewPrimaryResp{
			Status: NewStatus(OK),
		}
		return res, nil
	}
	// Do not have the chunk
	err = s.createChunkFile(chunkHandle, newPrimary)
	if err != nil {
		panic("failed to create new file on disk")
	}
	res = &pb.AssignNewPrimaryResp{
		Status: NewStatus(OK),
	}
	return res, nil
}

func (s *ChunkServer) createChunkFile(chunkHandle string, primaryAddress string) error {
	chunkLocation := path.Join(s.BasePath, chunkHandle)
	err := CreateFile(chunkLocation)
	if err != nil {
		// res := NewForwardCreateResp(ERROR_CREATE_CHUNK_FAILED)
		return err
		//return res, err
	}
	newChannel := make(chan string)
	log.Println("creating get version channel")
	newTimer := GetVersionTimer{
		Srv:         s,
		ChunkHandle: chunkHandle,
		Timeout:     100,
		Quit:        newChannel,
	}

	metadata := ChunkMetaData{ChunkLocation: chunkLocation, Role: Secondary, PrimaryChunkServer: primaryAddress, PeerAddress: nil, Used: 0, Version: 0, GetVersionChannel: newChannel}
	s.Chunks[chunkHandle] = &metadata

	go newTimer.Trigger()
	return nil
}

func (s *ChunkServer) UpdateBackup(ctx context.Context, req *pb.UpdateBackupReq) (*pb.UpdateBackupResp, error) {
	chunkHandle := req.GetChunkHandle()
	peers := req.GetPeers()

	meta, ok := s.Chunks[chunkHandle]
	if !ok {
		res := &pb.UpdateBackupResp{
			Status: NewStatus(ERROR_CHUNK_NOT_EXISTS),
		}
		return res, nil
	}

	role := meta.Role
	if role != Primary {
		res := &pb.UpdateBackupResp{
			Status: NewStatus(ERROR_NOT_PRIMARY),
		}
		return res, nil
	}

	// server is primary, overwrite peers
	meta.MetaDataLock.Lock()
	defer meta.MetaDataLock.Unlock()

	meta.PeerAddress = peers
	res := &pb.UpdateBackupResp{
		Status: NewStatus(OK),
	}
	return res, nil
}
