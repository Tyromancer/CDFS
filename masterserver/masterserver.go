package masterserver

import (
	"context"
	"fmt"
	"log"
	"math"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	cs "github.com/tyromancer/cdfs/chunkserver"
	pb "github.com/tyromancer/cdfs/pb"
)

type MasterServer struct {
	pb.UnimplementedMasterServer

	// a mapping from File name to A slice of HandleMetaData
	Files map[string][]*HandleMetaData

	// a mapping from chunk handle to HandleMetaData
	HandleToMeta map[string]*HandleMetaData

	// a map from the unique Token(Host:Port) of ChunkServer to its Used (sort on value Used)
	ChunkServerLoad map[string]uint

	// a mapping from ChunkServer to all HandleMetaData it has
	CSToHandle map[string][]*HandleMetaData

	// a map record hearbeat channel
	HeartBeatMap map[string]ChunkServerChan

	// globally unique server name
	ServerName string

	// base directory to store chunk files
	BasePath string

	HBMutex sync.Mutex
}

func (s *MasterServer) HeartBeat(ctx context.Context, heartBeatReq *pb.HeartBeatPayload) (*pb.HeartBeatResp, error) {
	chunkServerName := heartBeatReq.GetName()

	chanStruct := s.HeartBeatMap[chunkServerName]
	srvInfo := ChunkServerInfo{
		ChunkHandle: append([]string{}, heartBeatReq.ChunkHandle...),
		Used:        append([]uint32{}, heartBeatReq.Used...),
		Name:        chunkServerName,
	}
	//1. check isdead
	if s.HeartBeatMap[chunkServerName].isDead {
		//2. if dead, revive
		chanStruct.channel <- srvInfo
		s.CSToHandle[chunkServerName] = []*HandleMetaData{}
		return NewHeartBeatResp(ERROR_DEAD_BECOME_ALIVE), nil
	}
	//3. send message to detectHeartBeat
	chanStruct.channel <- srvInfo
	return NewHeartBeatResp(OK), nil
}

func (s *MasterServer) detectHeartBeat(chunkServerName string, heartbeat chan ChunkServerInfo) {
	timeout := 500 * time.Millisecond
	for {
		select {
		case heartBeatReq := <-heartbeat:
			chanStruct := s.HeartBeatMap[chunkServerName]
			if chanStruct.isDead {
				chanStruct.isDead = false
			} else {
				chunkHandles := heartBeatReq.ChunkHandle
				used := heartBeatReq.Used
				load := 0
				chunkServerName := heartBeatReq.Name
				for i, each := range chunkHandles {
					if _, ok := s.HandleToMeta[each]; !ok {
						continue
					}
					if s.HandleToMeta[each].PrimaryChunkServer == chunkServerName {
						s.HandleToMeta[each].Used = uint(used[i])
					}
					load += int(used[i])
				}
				s.HBMutex.Lock()
				s.ChunkServerLoad[chunkServerName] = uint(load)
				s.HBMutex.Unlock()
			}
		case <-time.After(timeout):
			//... no response
			// chunk server is dead
			chanStruct := s.HeartBeatMap[chunkServerName]
			if !chanStruct.isDead {
				s.handleChunkServerFailure(chunkServerName)
				chanStruct.isDead = true
			}
		}
	}
}

func (s *MasterServer) handleChunkServerFailure(chunkServerName string) {
	chunkHandles := s.CSToHandle[chunkServerName]
	//TODO: remove ChunkServer KV in CSToHandle map
	for _, each := range chunkHandles {
		// if role primary
		chunkHandle := each.ChunkHandle
		if each.PrimaryChunkServer == chunkServerName {
			s.handlePrimaryFailure(chunkHandle, chunkServerName)
		} else {
			// if role is backup
			s.handleBackupFailure(chunkHandle, chunkServerName)
		}
	}
	delete(s.CSToHandle, chunkServerName)
}

func (s *MasterServer) handleBackupFailure(chunkHandle string, chunkServerName string) {
	handleMeta := s.HandleToMeta[chunkHandle]

	sortedWithoutChunk := s.lowestAllChunkServer(chunkHandle)
	if len(sortedWithoutChunk) == 0 {
		return
	}
	newBackup := sortedWithoutChunk[0]

	// tell new backup
	var conn *grpc.ClientConn
	conn, err := grpc.Dial(newBackup, grpc.WithInsecure())
	defer conn.Close()
	if err != nil {
		return
	}
	c := pb.NewChunkServerClient(conn)
	reqBackup := &pb.AssignNewPrimaryReq{
		ChunkHandle: chunkHandle,
		Primary:     handleMeta.PrimaryChunkServer,
	}
	c.AssignNewPrimary(context.Background(), reqBackup)
	// update mapping
	s.CSToHandle[newBackup] = append(s.CSToHandle[newBackup], handleMeta)

	// construct new backupslice
	currBackup := handleMeta.BackupAddress
	oldBackup := ""
	for _, each := range currBackup {
		if each != chunkServerName {
			oldBackup = each
		}
	}
	newBackupSlice := []string{newBackup}
	if oldBackup != "" {
		newBackupSlice = append(newBackupSlice, oldBackup)
	}

	// tell primary
	var conn2 *grpc.ClientConn
	conn2, err = grpc.Dial(handleMeta.PrimaryChunkServer, grpc.WithInsecure())
	defer conn.Close()
	if err != nil {
		return
	}
	c = pb.NewChunkServerClient(conn2)
	reqUpdateBackup := &pb.UpdateBackupReq{
		ChunkHandle: chunkHandle,
		Peers:       newBackupSlice,
	}
	c.UpdateBackup(context.Background(), reqUpdateBackup)

	// update hanldeMetaData backup
	handleMeta.BackupAddress = newBackupSlice

}
func (s *MasterServer) handlePrimaryFailure(chunkHandle string, chunkServerName string) {
	handleMeta := s.HandleToMeta[chunkHandle]
	backupSlice := handleMeta.BackupAddress
	numBackup := len(backupSlice)
	newPrimary, err := checkVersion(backupSlice, chunkHandle)
	if newPrimary == "" || err != nil {
		// no backup
		return
	}
	sortedWithoutChunk := s.lowestAllChunkServer(chunkHandle)
	if len(sortedWithoutChunk) == 0 {
		return
	}

	// Send grpc Create to Primary Chunk Server
	var conn *grpc.ClientConn
	conn, err = grpc.Dial(newPrimary, grpc.WithInsecure())

	defer conn.Close()

	if err != nil {
		return
	}

	c := pb.NewChunkServerClient(conn)
	if numBackup == 1 {
		// find 2
		peers := sortedWithoutChunk[:min(len(sortedWithoutChunk), 2)]
		req := &pb.ChangeToPrimaryReq{
			ChunkHandle: chunkHandle,
			Role:        0,
			Peers:       peers,
		}
		res, _ := c.ChangeToPrimary(context.Background(), req)
		if res.GetStatus().GetStatusCode() == cs.ERROR_CHUNK_NOT_EXISTS {
			return
		}

		// tell new backup
		for _, peer := range peers {
			var conn *grpc.ClientConn
			conn, err = grpc.Dial(peer, grpc.WithInsecure())
			defer conn.Close()
			if err != nil {
				return
			}
			c := pb.NewChunkServerClient(conn)
			reqBackup := &pb.AssignNewPrimaryReq{
				ChunkHandle: chunkHandle,
				Primary:     newPrimary,
			}
			c.AssignNewPrimary(context.Background(), reqBackup)
			// update mapping
			s.CSToHandle[peer] = append(s.CSToHandle[peer], handleMeta)
		}
		handleMeta.PrimaryChunkServer = newPrimary
		handleMeta.BackupAddress = peers

	} else {
		// find 1
		var oldBackup string
		for _, each := range backupSlice {
			if each != newPrimary {
				oldBackup = each
			}
		}
		newBackup := sortedWithoutChunk[0]
		peers := []string{oldBackup, newBackup}
		req := &pb.ChangeToPrimaryReq{
			ChunkHandle: chunkHandle,
			Role:        0,
			Peers:       peers,
		}
		res, _ := c.ChangeToPrimary(context.Background(), req)
		if res.GetStatus().GetStatusCode() == cs.ERROR_CHUNK_NOT_EXISTS {
			return
		}

		// tell new backup
		var conn *grpc.ClientConn
		conn, err = grpc.Dial(newBackup, grpc.WithInsecure())
		defer conn.Close()
		if err != nil {
			return
		}
		c := pb.NewChunkServerClient(conn)
		reqBackup := &pb.AssignNewPrimaryReq{
			ChunkHandle: chunkHandle,
			Primary:     newPrimary,
		}
		c.AssignNewPrimary(context.Background(), reqBackup)
		// update mapping
		s.CSToHandle[newBackup] = append(s.CSToHandle[newBackup], handleMeta)

		handleMeta.PrimaryChunkServer = newPrimary
		handleMeta.BackupAddress = peers
	}
}

// chunk server <-> Master : ChunkServer Register, save the chunk server host and port
func (s *MasterServer) CSRegister(ctx context.Context, csRegisterReq *pb.CSRegisterReq) (*pb.CSRegisterResp, error) {
	csHost := csRegisterReq.GetHost()
	csPort := csRegisterReq.GetPort()
	csName := fmt.Sprintf("%s:%d", csHost, csPort)
	_, ok := s.ChunkServerLoad[csName]
	// if the ChunkServer already registered
	if ok {
		res := NewCSRegisterResp((ERROR_CHUNKSERVER_ALREADY_EXISTS))
		return res, nil
	}
	// Register the ChunkServer
	s.ChunkServerLoad[csName] = 0
	s.CSToHandle[csName] = []*HandleMetaData{}
	channel := make(chan ChunkServerInfo)
	s.HeartBeatMap[csName] = ChunkServerChan{
		isDead:  false,
		channel: channel,
	}
	go s.detectHeartBeat(csName, channel)
	return NewCSRegisterResp(OK), nil
}

// GetLocation return the IP of the Primary chunkserver and chunkID back to client
func (s *MasterServer) GetLocation(ctx context.Context, getLocationReq *pb.GetLocationReq) (*pb.GetLocationResp, error) {

	var csInfoSlice []*pb.ChunkServerInfo
	startOffSet := getLocationReq.GetOffset()
	endOffSet := getLocationReq.GetSize() + startOffSet
	// Use the given FileName to get the corresponding chunk handles
	fileName := getLocationReq.GetFileName()
	allHandles, exist := s.Files[fileName]
	if !exist {
		res := NewGetLocationResp(ERROR_FILE_NOT_EXISTS, []*pb.ChunkServerInfo{}, 0, 0)
		return res, nil
	}

	// find the start location -> start chunk index & start offset in start chunk
	startLoc := startLocation(allHandles, startOffSet)
	startChunkIndex := startLoc[0]
	startFinal := startLoc[1]

	// find the end location -> end chunk index & end offset in end chunk
	endLoc := endtLocation(allHandles, endOffSet)
	endChunkIndex := endLoc[0]
	endFinal := endLoc[1]

	toReadHandles := allHandles[startChunkIndex : endChunkIndex+1]
	for _, handleMeta := range toReadHandles {
		handle := handleMeta.ChunkHandle
		primary := handleMeta.PrimaryChunkServer
		// if the primary does not exist for the chunk handle, report error
		if primary == "" {
			res := NewGetLocationResp(ERROR_PRIMARY_NOT_EXISTS, []*pb.ChunkServerInfo{}, 0, 0)
			return res, nil
		}
		backup := handleMeta.BackupAddress
		newCSInfoMessage := &pb.ChunkServerInfo{
			ChunkHandle:    handle,
			PrimaryAddress: primary,
			BackupAddress:  backup,
		}
		csInfoSlice = append(csInfoSlice, newCSInfoMessage)
	}

	log.Printf("Find all the chunkservers to read given the FileName and ChunkIndex")
	return NewGetLocationResp(OK, csInfoSlice, startFinal, endFinal), nil
}

// client -> Master Create file given the FileName
func (s *MasterServer) Create(ctx context.Context, createReq *pb.CreateReq) (*pb.CreateResp, error) {

	fileName := createReq.GetFileName()

	// check if FileName already exists
	_, ok := s.Files[fileName]
	if ok {
		res := NewCreateResp(ERROR_FILE_ALREADY_EXISTS)
		return res, nil
	}

	// Get the 3(or less) chunk server with lowest Used
	lowestThree := lowestThreeChunkServer(s.ChunkServerLoad)

	// Get primary, if length 0 then no server available now
	var primary string
	if len(lowestThree) == 0 {
		res := NewCreateResp(ERROR_NO_SERVER_AVAILABLE)
		return res, nil
	} else {
		primary = lowestThree[0]
	}
	// Get peer, if no peer available return nil slice
	var peers []string
	if len(lowestThree) > 1 {
		peers = lowestThree[1:]
	} else {
		peers = []string{}
	}

	// Send grpc Create to Primary Chunk Server
	var conn *grpc.ClientConn
	conn, err := grpc.Dial(primary, grpc.WithInsecure())
	defer conn.Close()

	if err != nil {
		return NewCreateResp(ERROR_FAIL_TO_CONNECT_TO_CHUNKSERVER), nil
	}

	// TODO: Generate chunkHandle
	chunkHandle, err := GenerateToken(16)
	if err != nil {
		return NewCreateResp(ERROR_FAIL_TO_GENERATE_UNIQUE_TOKEN), nil
	}

	c := pb.NewChunkServerClient(conn)
	req := &pb.CreateChunkReq{
		ChunkHandle: chunkHandle,
		Role:        0,
		Primary:     primary,
		Peers:       peers,
	}
	res, err := c.CreateChunk(context.Background(), req)

	if err != nil || res.GetStatus().StatusCode != OK {
		return NewCreateResp(ERROR_FAIL_TO_CREATE_CHUNK_WHEN_CREATEFILE), nil
	}

	// update Files mapping
	handleMeta := HandleMetaData{
		ChunkHandle:        chunkHandle,
		PrimaryChunkServer: primary,
		BackupAddress:      peers,
		Used:               0,
	}
	s.Files[fileName] = []*HandleMetaData{&handleMeta}
	s.HandleToMeta[chunkHandle] = &handleMeta

	// update ChunkServer to HandleMetaData mapping
	s.CSToHandle[primary] = append(s.CSToHandle[primary], &handleMeta)
	for _, peer := range peers {
		s.CSToHandle[peer] = append(s.CSToHandle[peer], &handleMeta)
	}

	return NewCreateResp(OK), nil

}

// Client <-> Master : AppendFile request, given fileName and append size
func (s *MasterServer) AppendFile(ctx context.Context, appendFileReq *pb.AppendFileReq) (*pb.AppendFileResp, error) {
	fileName := appendFileReq.GetFileName()
	fileSize := appendFileReq.GetFileSize()
	allHandleMeta, exist := s.Files[fileName]
	if !exist {
		res := NewAppendFileResp(ERROR_FILE_NOT_EXISTS, []string{}, []string{})
		return res, nil
	}

	lastHandleMeta := allHandleMeta[len(allHandleMeta)-1]
	// if the last Chunk can fit data to append
	if fileSize <= ChunkSize-uint32(lastHandleMeta.Used) {
		// TODISC: Do I update the Used and ChunkServerLoad now or later
		lastHandleMeta.Used += uint(fileSize)
		s.ChunkServerLoad[lastHandleMeta.PrimaryChunkServer] += uint(fileSize)
		for i := 0; i < len(lastHandleMeta.BackupAddress); i++ {
			s.ChunkServerLoad[lastHandleMeta.BackupAddress[i]] += uint(fileSize)
		}
		res := NewAppendFileResp(OK, []string{lastHandleMeta.PrimaryChunkServer}, []string{lastHandleMeta.ChunkHandle})
		return res, nil
	}
	// Calculate the number of chunks to create to fit the append data
	numChunkToADD := int(math.Ceil(float64(fileSize) / float64(ChunkSize)))
	primarySlice := []string{}
	chunkHandleSlice := []string{}
	chunkHandle, err := GenerateToken(16)
	// TODISC: how to handle the error
	if err != nil {
		return NewAppendFileResp(ERROR_FAIL_TO_GENERATE_UNIQUE_TOKEN, []string{}, []string{}), nil
	}

	// if the last Chunk Used is 0 (One case is that the last chunk is just created from Create(), so empty chunk)
	if lastHandleMeta.Used == 0 {
		// TODISC: Update the Used and ChunkServerLoad now or later
		lastHandleMeta.Used = uint(ChunkSize)
		s.ChunkServerLoad[lastHandleMeta.PrimaryChunkServer] += uint(ChunkSize)
		for i := 0; i < len(lastHandleMeta.BackupAddress); i++ {
			s.ChunkServerLoad[lastHandleMeta.BackupAddress[i]] += uint(ChunkSize)
		}
		// since deal with last chunk now, remove from ToADD
		numChunkToADD -= 1
		fileSize -= ChunkSize
		primarySlice = append(primarySlice, lastHandleMeta.PrimaryChunkServer)
		chunkHandleSlice = append(chunkHandleSlice, chunkHandle)
	}

	for i := 0; i < numChunkToADD; i++ {
		// Get the 3(or less) chunk server with lowest Used
		lowestThree := lowestThreeChunkServer(s.ChunkServerLoad)

		// Get primary, if length 0 then no server available now
		var primary string
		if len(lowestThree) == 0 {
			res := NewAppendFileResp(ERROR_NO_SERVER_AVAILABLE, []string{}, []string{})
			return res, nil
		} else {
			primary = lowestThree[0]
		}
		// Get peer, if no peer available return nil slice
		var peers []string
		if len(lowestThree) > 1 {
			peers = lowestThree[1:]
		} else {
			peers = []string{}
		}

		// Send grpc Create to Primary Chunk Server
		var conn *grpc.ClientConn
		conn, err := grpc.Dial(primary, grpc.WithInsecure())
		defer conn.Close()

		if err != nil {
			return NewAppendFileResp(ERROR_FAIL_TO_CONNECT_TO_CHUNKSERVER, []string{}, []string{}), nil
		}

		chunkHandle, err := GenerateToken(16)
		// TODISC: how to handle the error
		if err != nil {
			return NewAppendFileResp(ERROR_FAIL_TO_GENERATE_UNIQUE_TOKEN, []string{}, []string{}), nil
		}

		c := pb.NewChunkServerClient(conn)
		req := &pb.CreateChunkReq{
			ChunkHandle: chunkHandle,
			Role:        0,
			Peers:       peers,
		}
		res, err := c.CreateChunk(context.Background(), req)

		if err != nil || res.GetStatus().StatusCode != OK {
			return NewAppendFileResp(ERROR_FAIL_TO_CREATE_CHUNK_WHEN_APPEND, []string{}, []string{}), nil
		}

		// update Files and ChunkServerLoad mapping
		used := uint(ChunkSize)
		if fileSize < ChunkSize {
			used = uint(fileSize)
		}
		handleMeta := HandleMetaData{
			ChunkHandle:        chunkHandle,
			PrimaryChunkServer: primary,
			BackupAddress:      peers,
			Used:               used,
		}
		// update mapping
		s.Files[fileName] = append(s.Files[fileName], &handleMeta)
		s.HandleToMeta[chunkHandle] = &handleMeta
		s.CSToHandle[primary] = append(s.CSToHandle[primary], &handleMeta)
		for _, peer := range peers {
			s.CSToHandle[peer] = append(s.CSToHandle[peer], &handleMeta)
		}

		s.ChunkServerLoad[handleMeta.PrimaryChunkServer] += used
		for i := 0; i < len(handleMeta.BackupAddress); i++ {
			s.ChunkServerLoad[handleMeta.BackupAddress[i]] += used
		}
		// add primary and backup to slices which is used in AppendFileResp
		primarySlice = append(primarySlice, handleMeta.PrimaryChunkServer)
		chunkHandleSlice = append(chunkHandleSlice, chunkHandle)

		fileSize -= uint32(used)
	}
	res := NewAppendFileResp(OK, primarySlice, chunkHandleSlice)
	return res, nil
}

/*
Client <-> Master : Master return a unique token for each client
First message client send to master
*/
func (s *MasterServer) GetToken(ctx context.Context, getTokenReq *pb.GetTokenReq) (*pb.GetTokenResp, error) {
	token, err := GenerateToken(16)
	if err != nil {
		return NewGetTokenResp(""), status.Errorf(codes.NotFound, "Error when generating token")
	}
	return NewGetTokenResp(token), nil

}

func (s *MasterServer) deleteCSToChunk(handleMeta *HandleMetaData, chunkServer string) {
	sliceOfHandleMeta := s.CSToHandle[chunkServer]
	i := 0
	for ; i < len(sliceOfHandleMeta); i++ {
		if sliceOfHandleMeta[i] == handleMeta {
			break
		}
	}
	sliceOfHandleMeta = append(sliceOfHandleMeta[:i], sliceOfHandleMeta[i+1:]...)
	s.CSToHandle[chunkServer] = sliceOfHandleMeta
}

// Client <-> Master : Delete a file given the filename
func (s *MasterServer) Delete(ctx context.Context, deleteReq *pb.DeleteReq) (*pb.DeleteStatus, error) {
	fileName := deleteReq.GetFileName()
	allHandles, exist := s.Files[fileName]
	if !exist {
		res := NewDeleteStatus(ERROR_FILE_NOT_EXISTS)
		return res, nil
	}
	delete(s.Files, fileName)

	// for each loop to delete each chunk
	for _, handleMeta := range allHandles {
		chunkHandle := handleMeta.ChunkHandle
		primary := handleMeta.PrimaryChunkServer
		peers := handleMeta.BackupAddress
		if primary == "" {
			res := NewDeleteStatus(ERROR_PRIMARY_NOT_EXISTS)
			return res, nil
		}
		// Use helper function to delete the chunk
		err := DeleteChunkHandle(primary, chunkHandle)

		// delete chunkHandle mapping
		delete(s.HandleToMeta, chunkHandle)
		s.deleteCSToChunk(handleMeta, primary)
		for _, peer := range peers {
			DeleteChunkHandle(peer, chunkHandle)
			s.deleteCSToChunk(handleMeta, peer)
		}

		if err != nil {
			res := NewDeleteStatus(ERROR_FAIL_TO_DELETE)
			return res, nil
		}
	}
	res := NewDeleteStatus(OK)
	return res, nil
}

// chunk server <-> Master : modify used and load based on append outcome
func (s *MasterServer) AppendResult(ctx context.Context, appendResultReq *pb.AppendResultReq) (*pb.AppendResultResp, error) {
	statusCode := appendResultReq.GetStatus().GetStatusCode()
	chunkHandle := appendResultReq.GetChunkHandle()
	size := appendResultReq.GetSize()

	// if append fail, modify chunkMeta Used & chunkserver load
	if statusCode != OK {
		chunkMetaData := s.HandleToMeta[chunkHandle]
		chunkMetaData.Used -= uint(size)
		primary := chunkMetaData.PrimaryChunkServer
		s.ChunkServerLoad[primary] -= uint(size)
		for i := 0; i < len(chunkMetaData.BackupAddress); i++ {
			s.ChunkServerLoad[chunkMetaData.BackupAddress[i]] -= uint(size)
		}
	}

	return NewAppendResultResp(), nil
}
