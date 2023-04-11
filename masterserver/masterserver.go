package masterserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-redis/redis/v8"
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

	FileMutex            sync.Mutex
	CSToHandleMutex      sync.Mutex
	ChunkServerLoadMutex sync.Mutex

	// a mapping from File name to A slice of HandleMetaData
	Files map[string][]*HandleMetaData

	// a mapping from chunk handle to HandleMetaData
	HandleToMeta map[string]*HandleMetaData

	// a map from the unique Token(Host:Port) of ChunkServer to its Used (sort on value Used)
	ChunkServerLoad map[string]uint

	// a mapping from ChunkServer to all HandleMetaData it has
	CSToHandle map[string][]*HandleMetaData

	// a map record hearbeat channel
	HeartBeatMap map[string]*ChunkServerChan

	// globally unique server name
	ServerName string

	// base directory to store chunk files
	BasePath string

	HBMutex sync.Mutex

	DB *redis.Client
}

func (s *MasterServer) PersistState(filename string, operation int) {
	isLocked := s.FileMutex.TryLock()
	if !isLocked {
		log.Printf("failed to acquire files lock")
		return
	}
	defer s.FileMutex.Unlock()

	var err error
	if operation == DB_SET {
		handles, ok := s.Files[filename]
		if !ok {
			log.Printf("%s not in masterserver.files")
			return

		}
		serializedSlice, err := json.Marshal(handles)
		if err != nil {
			log.Printf("failed to marshal value with key %s", filename)
			return
		}
		err = s.DB.HSet(context.Background(), "files", filename, serializedSlice).Err()
	} else {
		err = s.DB.HDel(context.Background(), "files", filename).Err()
	}

	if err != nil {
		log.Printf("failed to do operation for %s", filename)
		return
	}

	log.Printf("DB operation success for %s", filename)
	//for key, val := range s.Files {
	//	serializedSlice, err := json.Marshal(val)
	//	if err != nil {
	//		log.Printf("failed to marshal value with key %s", key)
	//		return
	//	}
	//	err = s.DB.HSet(context.Background(), "files", key, serializedSlice).Err()
	//	if err != nil {
	//		log.Printf("failed to set value with key %s", key)
	//		return
	//	}
	//}
}

func (s *MasterServer) HeartBeat(ctx context.Context, heartBeatReq *pb.HeartBeatPayload) (*pb.HeartBeatResp, error) {
	chunkServerName := heartBeatReq.GetName()

	chanStruct, ok := s.HeartBeatMap[chunkServerName]
	if !ok {
		// Master get re-boot, receive heartbeat
		channel := make(chan ChunkServerInfo)
		s.HeartBeatMap[chunkServerName] = &ChunkServerChan{
			isDead:  false,
			channel: channel,
		}
		// TODO: Need to send message to channel or not?
		go s.detectHeartBeat(chunkServerName, channel)
		// TODO: set entry in CSToHandle
		// TODO: set ChunkServerLoad
		chunkHandles := heartBeatReq.GetChunkHandle()
		used := heartBeatReq.GetUsed()
		newCSValue := []*HandleMetaData{}
		var load uint32
		for i, chunkHandle := range chunkHandles {
			newCSValue = append(newCSValue, s.HandleToMeta[chunkHandle])
			load += used[i]
		}
		s.HBMutex.Lock()
		s.CSToHandle[chunkServerName] = newCSValue
		s.ChunkServerLoad[chunkServerName] = uint(load)
		s.HBMutex.Unlock()

		channel <- ChunkServerInfo{
			ChunkHandle: chunkHandles,
			Used:        used,
			Name:        chunkServerName,
		}

		return NewHeartBeatResp(OK), nil
	}

	srvInfo := ChunkServerInfo{
		ChunkHandle: append([]string{}, heartBeatReq.ChunkHandle...),
		Used:        append([]uint32{}, heartBeatReq.Used...),
		Name:        chunkServerName,
	}
	//1. check isdead, this chunk server just recover from a previous lost, treated as a new idle chunk server
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
			//chanStruct := s.HeartBeatMap[chunkServerName]
			if s.HeartBeatMap[chunkServerName].isDead {
				s.handleChunkServerFailure(chunkServerName)
				s.HeartBeatMap[chunkServerName].isDead = true
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

	// TODO: only clear value pair from CSToHandle, delete key from used map
	delete(s.CSToHandle, chunkServerName)
	delete(s.ChunkServerLoad, chunkServerName)
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

		var oldBackupConn *grpc.ClientConn
		oldBackupConn, err := grpc.Dial(oldBackup, grpc.WithInsecure())
		defer oldBackupConn.Close()
		if err != nil {
			return
		}
		oldBackupClient := pb.NewChunkServerClient(oldBackupConn)
		oldBackupClient.AssignNewPrimary(context.Background(), reqBackup)

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
	s.HeartBeatMap[csName] = &ChunkServerChan{
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
	endOffSet := getLocationReq.GetSize()
	if endOffSet != 0 {
		endOffSet += startOffSet
	}

	// Use the given FileName to get the corresponding chunk handles
	fileName := getLocationReq.GetFileName()
	allHandles, exist := s.Files[fileName]
	if !exist {
		res := NewGetLocationResp(ERROR_FILE_NOT_EXISTS, []*pb.ChunkServerInfo{}, 0, 0)
		return res, nil
	}

	// find the start location -> start chunk index & start offset in start chunk
	startChunkIndex, startFinal, err := startLocation(allHandles, startOffSet)
	if err != nil {
		res := NewGetLocationResp(ERROR_READ_WRONG_OFFSET, nil, 0, 0)
		return res, nil
	}

	// find the end location -> end chunk index & end offset in end chunk
	endChunkIndex, endFinal, err := endLocation(allHandles, endOffSet)
	if err != nil {
		res := NewGetLocationResp(ERROR_READ_WRONG_SIZE, nil, 0, 0)
		return res, nil
	}

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
	s.PersistState(fileName, DB_SET)
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
	log.Printf("numChunkAdd is %d", numChunkToADD)
	primarySlice := []string{}
	chunkHandleSlice := []string{}
	//chunkHandle, err := GenerateToken(16)
	//log.Printf("generated chunk handle: %s", chunkHandle)
	// TODISC: how to handle the error
	//if err != nil {
	//	return NewAppendFileResp(ERROR_FAIL_TO_GENERATE_UNIQUE_TOKEN, []string{}, []string{}), nil
	//}

	// if the last Chunk Used is 0 (One case is that the last chunk is just created from Create(), so empty chunk)
	if lastHandleMeta.Used == 0 {
		log.Printf("last handle meta used is 0")
		// TODISC: Update the Used and ChunkServerLoad now or later
		lastHandleMeta.Used = uint(ChunkSize)
		log.Printf("last handle meta used now is %d", lastHandleMeta.Used)

		s.ChunkServerLoad[lastHandleMeta.PrimaryChunkServer] += uint(ChunkSize)
		for i := 0; i < len(lastHandleMeta.BackupAddress); i++ {
			s.ChunkServerLoad[lastHandleMeta.BackupAddress[i]] += uint(ChunkSize)
		}
		// since deal with last chunk now, remove from ToADD
		numChunkToADD -= 1
		fileSize -= ChunkSize
		primarySlice = append(primarySlice, lastHandleMeta.PrimaryChunkServer)
		//chunkHandleSlice = append(chunkHandleSlice, chunkHandle)
		chunkHandleSlice = append(chunkHandleSlice, lastHandleMeta.ChunkHandle)

	}

	log.Printf("now chunk to add is %d", numChunkToADD)
	workerChannels := make([]chan int32, numChunkToADD)
	usedSlice := make([]uint, numChunkToADD)
	for i := 0; i < numChunkToADD; i++ {
		workerChannels[i] = make(chan int32)
		if i == numChunkToADD-1 {
			usedSlice[i] = uint(fileSize) - uint(ChunkSize)*uint(i)
		} else {
			usedSlice[i] = uint(ChunkSize)
		}
	}
	var wg sync.WaitGroup
	wg.Add(numChunkToADD)

	for i := 0; i < numChunkToADD; i++ {
		// Get the 3(or less) chunk server with lowest Used
		lowestThree := lowestThreeChunkServer(s.ChunkServerLoad)
		log.Println("lowest 3 is ", lowestThree)
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

		chunkHandle, err := GenerateToken(16)
		if err != nil {
			res := NewAppendFileResp(ERROR_FAIL_TO_GENERATE_UNIQUE_TOKEN, []string{}, []string{})
			return res, nil
		}

		go s.AppendWorker(fileName, fileSize, primary, peers, chunkHandle, usedSlice[i], workerChannels[i], &wg)
		//// Send grpc Create to Primary Chunk Server
		//var conn *grpc.ClientConn
		//conn, err := grpc.Dial(primary, grpc.WithInsecure())
		//defer conn.Close()
		//
		//if err != nil {
		//	return NewAppendFileResp(ERROR_FAIL_TO_CONNECT_TO_CHUNKSERVER, []string{}, []string{}), nil
		//}
		//
		//chunkHandle, err := GenerateToken(16)
		//// TODISC: how to handle the error
		//if err != nil {
		//	return NewAppendFileResp(ERROR_FAIL_TO_GENERATE_UNIQUE_TOKEN, []string{}, []string{}), nil
		//}
		//
		//c := pb.NewChunkServerClient(conn)
		//req := &pb.CreateChunkReq{
		//	ChunkHandle: chunkHandle,
		//	Role:        0,
		//	Peers:       peers,
		//}
		//res, err := c.CreateChunk(context.Background(), req)
		//
		//if err != nil || res.GetStatus().StatusCode != OK {
		//	return NewAppendFileResp(ERROR_FAIL_TO_CREATE_CHUNK_WHEN_APPEND, []string{}, []string{}), nil
		//}
		//
		//// update Files and ChunkServerLoad mapping
		//used := uint(ChunkSize)
		//if fileSize < ChunkSize {
		//	used = uint(fileSize)
		//}
		//handleMeta := HandleMetaData{
		//	ChunkHandle:        chunkHandle,
		//	PrimaryChunkServer: primary,
		//	BackupAddress:      peers,
		//	Used:               used,
		//}
		//// update mapping
		//s.Files[fileName] = append(s.Files[fileName], &handleMeta)
		//s.PersistState(fileName, DB_SET)
		//s.HandleToMeta[chunkHandle] = &handleMeta
		//s.CSToHandle[primary] = append(s.CSToHandle[primary], &handleMeta)
		//for _, peer := range peers {
		//	s.CSToHandle[peer] = append(s.CSToHandle[peer], &handleMeta)
		//}
		//
		//s.ChunkServerLoad[handleMeta.PrimaryChunkServer] += used
		//for i := 0; i < len(handleMeta.BackupAddress); i++ {
		//	s.ChunkServerLoad[handleMeta.BackupAddress[i]] += used
		//}
		//// add primary and backup to slices which is used in AppendFileResp
		//primarySlice = append(primarySlice, handleMeta.PrimaryChunkServer)
		//chunkHandleSlice = append(chunkHandleSlice, chunkHandle)
		//
		//fileSize -= uint32(used)
	}
	wg.Wait()
	log.Printf("done waiting")
	for i := 0; i < numChunkToADD; i++ {
		statusCode := <-workerChannels[i]
		log.Printf("appendfile: got status %s", ErrorCodeToString(statusCode))
		if statusCode != OK {
			res := NewAppendFileResp(statusCode, []string{}, []string{})
			return res, nil
		}
	}
	res := NewAppendFileResp(OK, primarySlice, chunkHandleSlice)
	log.Printf("send result to client: %s", res.GetStatus().GetErrorMessage())
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
	s.PersistState(fileName, DB_DELETE)

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
	log.Printf("recvd append result for chunk %s", chunkHandle)

	// if append fail, modify chunkMeta Used & chunkserver load
	if statusCode != OK {
		chunkMetaData, ok := s.HandleToMeta[chunkHandle]
		if !ok {
			log.Printf("chunk handle is not stored in memory")
		}
		chunkMetaData.Used -= uint(size)
		primary := chunkMetaData.PrimaryChunkServer
		s.ChunkServerLoad[primary] -= uint(size)
		for i := 0; i < len(chunkMetaData.BackupAddress); i++ {
			s.ChunkServerLoad[chunkMetaData.BackupAddress[i]] -= uint(size)
		}
	}

	return NewAppendResultResp(), nil
}

// GenerateHandleToMetaMap: when master server re-boot from crash, generate the HandleToMeta map from Files map.
func (s *MasterServer) GenerateHandleToMetaMap() error {
	if len(s.Files) == 0 {
		log.Println("There is no file")
		return nil
	}

	for _, fileChunkHandles := range s.Files {
		for _, chunkMetaData := range fileChunkHandles {
			chunkHandle := chunkMetaData.ChunkHandle
			_, ok := s.HandleToMeta[chunkHandle]
			if !ok {
				log.Println("Find a chunk handle twice")
				return errors.New("find a duplicate chunk handle")
			}
			s.HandleToMeta[chunkHandle] = chunkMetaData
		}
	}
	return nil
}

func (s *MasterServer) AppendWorker(fileName string, fileSize uint32, primary string, peers []string, chunkHandle string, used uint, result chan int32, wg *sync.WaitGroup) {
	//lowestThree := lowestThreeChunkServer(s.ChunkServerLoad)
	defer func() {
		wg.Done()
		log.Printf("wg Done by 1")
	}()
	log.Printf("entered append worker for chunk %s with primary %s", chunkHandle, primary)
	var conn *grpc.ClientConn
	conn, err := grpc.Dial(primary, grpc.WithInsecure())
	defer conn.Close()

	if err != nil {
		log.Printf("chunk %s: failed to connect to chunk server", chunkHandle)
		result <- ERROR_FAIL_TO_CONNECT_TO_CHUNKSERVER
		return
	}

	c := pb.NewChunkServerClient(conn)
	req := &pb.CreateChunkReq{
		ChunkHandle: chunkHandle,
		Role:        0,
		Peers:       peers,
		Primary:     primary,
	}
	res, err := c.CreateChunk(context.Background(), req)
	if err != nil || res.GetStatus().StatusCode != OK {
		log.Printf("chunk %s: failed to create chunk", chunkHandle)

		result <- ERROR_FAIL_TO_CREATE_CHUNK_WHEN_APPEND
		return
		//return NewAppendFileResp(ERROR_FAIL_TO_CREATE_CHUNK_WHEN_APPEND, []string{}, []string{}), nil
	}
	log.Printf("chunk %s: create chunk success", chunkHandle)

	handleMeta := HandleMetaData{
		ChunkHandle:        chunkHandle,
		PrimaryChunkServer: primary,
		BackupAddress:      peers,
		Used:               used,
	}

	s.Files[fileName] = append(s.Files[fileName], &handleMeta)
	//s.PersistState(fileName, DB_SET)
	s.HandleToMeta[chunkHandle] = &handleMeta
	log.Printf("chunk %s: begin acquire locks", chunkHandle)
	s.CSToHandleMutex.Lock()
	s.ChunkServerLoadMutex.Lock()
	defer s.ChunkServerLoadMutex.Unlock()
	defer s.CSToHandleMutex.Unlock()
	log.Printf("chunk %s: acquired two locks", chunkHandle)

	s.CSToHandle[primary] = append(s.CSToHandle[primary], &handleMeta)
	for _, peer := range peers {
		s.CSToHandle[peer] = append(s.CSToHandle[peer], &handleMeta)
	}
	s.ChunkServerLoad[handleMeta.PrimaryChunkServer] += used
	for i := 0; i < len(handleMeta.BackupAddress); i++ {
		s.ChunkServerLoad[handleMeta.BackupAddress[i]] += used
	}
	log.Printf("chunk %s: worker done", chunkHandle)
	result <- OK
	return
}
