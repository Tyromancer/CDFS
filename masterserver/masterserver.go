package masterserver

import (
	"context"
	"errors"
	"log"

	"google.golang.org/grpc"

	pb "github.com/tyromancer/cdfs/pb"
)

type MasterServer struct {
	pb.UnimplementedMasterServer

	// a mapping from File name to A slice of HandleMetaData
	Files map[string][]HandleMetaData

	// a map from the unique Token(Host:Port) of ChunkServer to its Used (sort on value Used)
	ChunkServerLoad map[string]uint

	// globally unique server name
	ServerName string

	// base directory to store chunk files
	BasePath string
}



// GetLocation return the IP of the Primary chunkserver and chunkID back to client
func (s *MasterServer) GetLocation(ctx context.Context, getLocationReq *pb.GetLocationReq) (*pb.GetLocationResp, error) {

	// Use the given FileName to get the corresponding chunk handles
	fileName := getLocationReq.GetFileName()
	allHandles, exist := s.Files[fileName]
	if !exist {
		res := NewGetLocationResp(ERROR_FILE_NOT_EXISTS, "", "")
		return res, errors.New(res.GetStatus().ErrorMessage)
	}

	// Use ChunkIndex to find the Handle that client asks for
	chunkIndex := getLocationReq.GetChunkIndex()
	handleMeta := allHandles[chunkIndex]
	primaryIP := handleMeta.PrimaryChunkServer

	// if the primary does not exist for the chunk handle, report error
	if primaryIP == "" {
		res := NewGetLocationResp(ERROR_PRIMARY_NOT_EXISTS, "", "")
		return res, errors.New(res.GetStatus().ErrorMessage)
	}
	log.Printf("Find the primary chunk server given the FileName and ChunkIndex")
	return NewGetLocationResp(OK, primaryIP, handleMeta.ChunkHandle), nil
}


// // client -> Master Create file given the FileName
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
	chunkHandle := "0"
	defer conn.Close()

	c := pb.NewChunkServerClient(conn)
	req := &pb.CreateChunkReq{ChunkHandle: chunkHandle, Role: 0, Peers: peers}
	res, err := c.CreateChunk(context.Background(), req)

	if err != nil {
		log.Fatalf("Error when calling CreateChunk: %v", err)
	}

	if res.GetStatus().StatusCode == OK {
		return NewCreateResp(res.GetStatus().StatusCode), nil
	} else {
		return NewCreateResp(res.GetStatus().StatusCode), errors.New(res.GetStatus().ErrorMessage)
	}
	
}


