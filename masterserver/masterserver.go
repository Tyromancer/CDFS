package masterserver

import (
	"context"
	"errors"
	"log"

	pb "github.com/tyromancer/cdfs/pb"
)

type MasterServer struct {
	pb.UnimplementedMasterServer

	// a mapping from File name to A slice of chunk handles
	Files map[string][]string

	/* a mapping from chunk handles to primary, list of chunkservers
	Side Note: using interface here because the 1st element is primaryIP string,
	the 2nd element is a slice of chunkservers. */
	Handles map[string][]interface{}

	// globally unique server name
	ServerName string

	// base directory to store chunk files
	BasePath string
}



// GetLocation return the IP of the Primary chunkserver and chunkID back to client
func (s *MasterServer) GetLocation(ctx context.Context, getLocationReq *pb.GetLocationReq) (*pb.GetLocationResp, error) {

	// Use the given FileName to get the corresponding chunk handles
	fileName := getLocationReq.GetFileName()
	allHandles, notexist := s.Files[fileName]
	if notexist {
		res := NewGetLocationResp(ERROR_FILE_NOT_EXISTS, "", "")
		return res, errors.New(res.GetStatus().ErrorMessage)
	}

	// Use ChunkIndex to find the Handle that client asks for
	chunkIndex := getLocationReq.GetChunkIndex()
	handle := allHandles[chunkIndex]
	chunkServerInfo := s.Handles[handle]
	primaryIP := chunkServerInfo[0].(string)

	// if the primary does not exist for the chunk handle, report error
	if primaryIP == "" {
		res := NewGetLocationResp(ERROR_PRIMARY_NOT_EXISTS, "", "")
		return res, errors.New(res.GetStatus().ErrorMessage)
	}
	log.Printf("Find the primary chunk server given the FileName and ChunkIndex")
	return NewGetLocationResp(OK, primaryIP, handle), nil
	
}
