package masterserver

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"

	"google.golang.org/grpc"

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

	// globally unique server name
	ServerName string

	// base directory to store chunk files
	BasePath string
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
		return res, errors.New(res.GetStatus().ErrorMessage)
	}
	// Register the ChunkServer
	s.ChunkServerLoad[csName] = 0

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
		return res, errors.New(res.GetStatus().ErrorMessage)
	}

	// find the start location -> start chunk index & start offset in start chunk
	startLoc := startLocation(allHandles, startOffSet)
	startChunkIndex := startLoc[0]
	startFinal := startLoc[1]

	// find the end location -> end chunk index & end offset in end chunk
	endLoc := endtLocation(allHandles, endOffSet)
	endChunkIndex := endLoc[0]
	endFinal := endLoc[1]

	toReadHandles := allHandles[startChunkIndex:endChunkIndex+1]
	for _, handleMeta := range toReadHandles {
		handle := handleMeta.ChunkHandle
		primary := handleMeta.PrimaryChunkServer
		// if the primary does not exist for the chunk handle, report error
		if primary == "" {
			res := NewGetLocationResp(ERROR_PRIMARY_NOT_EXISTS, []*pb.ChunkServerInfo{}, 0, 0)
			return res, errors.New(res.GetStatus().ErrorMessage)
		}
		backup := handleMeta.BackupAddress
		newCSInfoMessage := &pb.ChunkServerInfo{
			ChunkHandle: handle, 
			PrimaryAddress: primary, 
			BackupAddress: backup,
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
		return res, errors.New(res.GetStatus().ErrorMessage)
	}

	// Get the 3(or less) chunk server with lowest Used
	lowestThree := lowestThreeChunkServer(s.ChunkServerLoad)

	// Get primary, if length 0 then no server available now
	var primary string
	if len(lowestThree) == 0 {
		res := NewCreateResp(ERROR_NO_SERVER_AVAILABLE)
		return res, errors.New(res.GetStatus().ErrorMessage)
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

	if err != nil {
		log.Fatalf("Failed to connect to Chunk Server: %v", err)
	}

	// TODO: Generate chunkHandle
	chunkHandle, err := GenerateToken(16)
	if err != nil {
		log.Fatalf("Error when generate token: %v", err)
	}
	defer conn.Close()

	c := pb.NewChunkServerClient(conn)
	req := &pb.CreateChunkReq{
		ChunkHandle: chunkHandle, 
		Role: 0, 
		Primary: primary, 
		Peers: peers,
	}
	res, err := c.CreateChunk(context.Background(), req)

	if err != nil {
		log.Fatalf("Error when calling CreateChunk: %v", err)
	}

	// update Files mapping
	handleMeta := HandleMetaData{
		ChunkHandle: chunkHandle, 
		PrimaryChunkServer: primary, 
		BackupAddress: peers, 
		Used: 0,
	}
	s.Files[fileName] = []*HandleMetaData{&handleMeta}
	s.HandleToMeta[chunkHandle] = &handleMeta

	if res.GetStatus().StatusCode == OK {
		return NewCreateResp(res.GetStatus().StatusCode), nil
	} else {
		return NewCreateResp(res.GetStatus().StatusCode), errors.New(res.GetStatus().ErrorMessage)
	}

}



// Client <-> Master : AppendFile request, given fileName and append size
func (s *MasterServer) AppendFile(ctx context.Context, appendFileReq *pb.AppendFileReq) (*pb.AppendFileResp, error) {
	fileName := appendFileReq.GetFileName()
	fileSize := appendFileReq.GetFileSize()
	allHandleMeta, exist := s.Files[fileName]
	if !exist {
		res := NewAppendFileResp(ERROR_FILE_NOT_EXISTS, []string{}, []string{})
		return res, errors.New(res.GetStatus().ErrorMessage)
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
		log.Fatalf("Error when generate token: %v", err)
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
			return res, errors.New(res.GetStatus().ErrorMessage)
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

		if err != nil {
			log.Fatalf("Failed to connect to Chunk Server: %v", err)
		}

		chunkHandle, err := GenerateToken(16)
		// TODISC: how to handle the error
		if err != nil {
			log.Fatalf("Error when generate token: %v", err)
		}
		defer conn.Close()

		c := pb.NewChunkServerClient(conn)
		req := &pb.CreateChunkReq{
			ChunkHandle: chunkHandle, 
			Role: 0, 
			Peers: peers,
		}
		res, err := c.CreateChunk(context.Background(), req)

		if err != nil {
			log.Fatalf("Error when calling CreateChunk: %v", err)
		}

		// What to if the CreateChunkResp status code is not OK? Retry?
		if res.GetStatus().StatusCode != OK {
			//TODO
		}

		// update Files and ChunkServerLoad mapping
		used := uint(ChunkSize)
		if fileSize < ChunkSize {
			used = uint(fileSize)
		}
		handleMeta := HandleMetaData{
			ChunkHandle: chunkHandle, 
			PrimaryChunkServer: primary, 
			BackupAddress: peers, 
			Used: used,
		}
		s.Files[fileName] = append(s.Files[fileName], &handleMeta)
		s.HandleToMeta[chunkHandle] = &handleMeta

		s.ChunkServerLoad[lastHandleMeta.PrimaryChunkServer] += used
		for i := 0; i < len(lastHandleMeta.BackupAddress); i++ {
			s.ChunkServerLoad[lastHandleMeta.BackupAddress[i]] += used
		}
		// add primary and backup to slices which is used in AppendFileResp
		primarySlice = append(primarySlice, lastHandleMeta.PrimaryChunkServer)
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
		log.Fatalf("Error when generating token: %v", err)
        return NewGetTokenResp(""), errors.New(ErrorCodeToString(ERROR_FAIL_TO_GENERATE_UNIQUE_TOKEN))
    }
	return NewGetTokenResp(token), nil
	
}




// Client <-> Master : Delete a file given the filename
func (s *MasterServer) 	Delete(ctx context.Context, deleteReq *pb.DeleteReq) (*pb.DeleteStatus, error) {
	fileName := deleteReq.GetFileName()
	allHandles, exist := s.Files[fileName]
	if !exist {
		res := NewDeleteStatus(ERROR_FILE_NOT_EXISTS)
		return res, errors.New(res.GetStatus().ErrorMessage)
	}

	// for each loop to delete each chunk
	for _, handleMeta := range allHandles {
		chunkHandle := handleMeta.ChunkHandle
		primary := handleMeta.PrimaryChunkServer
		if primary == "" {
			res := NewDeleteStatus(ERROR_PRIMARY_NOT_EXISTS)
			return res, errors.New(res.GetStatus().ErrorMessage)
		}
		// Use helper function to delete the chunk
		err := DeleteChunkHandle(primary, chunkHandle)
		if err != nil {
			res := NewDeleteStatus(ERROR_FAIL_TO_DELETE)
			return res, err
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
		chunkMetaData.Used -= uint (size)
		primary := chunkMetaData.PrimaryChunkServer
		s.ChunkServerLoad[primary] -= uint (size)
		for i := 0; i < len(chunkMetaData.BackupAddress); i++ {
			s.ChunkServerLoad[chunkMetaData.BackupAddress[i]] -= uint (size)
		}
	}

	return NewAppendResultResp(), nil
}

