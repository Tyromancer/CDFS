package masterserver

import (
	"fmt"
	"github.com/tyromancer/cdfs/pb"
	
)

const (
	OK int32 = iota
	ERROR_FILE_NOT_EXISTS
	ERROR_PRIMARY_NOT_EXISTS

	
)

func ErrorCodeToString(e int32) string {
	switch e {
	case OK:
		return "OK"
	case ERROR_FILE_NOT_EXISTS:
		return "Error: the given FileName does not exist"
	case ERROR_PRIMARY_NOT_EXISTS:
		return "Error: the primary does not exist for the chunk handle"
	
	default:
		return fmt.Sprintf("%d", int(e))
	}
}


func NewGetLocationResp(errorCode int32, primaryIP string, chunckHandle string) *pb.GetLocationResp {
	return &pb.GetLocationResp{Status: &pb.Status{StatusCode: errorCode, ErrorMessage: ErrorCodeToString(errorCode)}, PrimaryIP: primaryIP, ChunkHandle: chunckHandle}
}