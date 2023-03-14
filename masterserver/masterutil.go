package masterserver

import (
	"fmt"
	"github.com/tyromancer/cdfs/pb"
	"sort"
	
)

const (
	OK int32 = iota
	ERROR_FILE_NOT_EXISTS
	ERROR_PRIMARY_NOT_EXISTS
	ERROR_FILE_ALREADY_EXISTS
	ERROR_NO_SERVER_AVAILABLE

	
)

func ErrorCodeToString(e int32) string {
	switch e {
	case OK:
		return "OK"
	case ERROR_FILE_NOT_EXISTS:
		return "Error: the given FileName does not exist"
	case ERROR_PRIMARY_NOT_EXISTS:
		return "Error: the primary does not exist for the chunk handle"
	case ERROR_FILE_ALREADY_EXISTS:
		return "Error: the given FileName does not exist"
	default:
		return fmt.Sprintf("%d", int(e))
	}
}


type HandleMetaData struct {
	// unique chunk handle of the chunk
	ChunkHandle string

	// the IP address of Primary chunk server of the chunk
	PrimaryChunkServer string

	// IP address of backup chunk server for this chunk
	BackupAddress []string

	// Already used size in bytes
	Used uint
}

// Pair represents a key-value pair. For sorting the map
type Pair struct {
	key   string
	value uint
}

// return the three(or less) chunkservers that have the lowest load given the ChunkServerLoad map
func lowestThreeChunkServer(chunkServerLoad map[string]uint) []string {
	var pairs []Pair
	for k, v := range chunkServerLoad {
		pairs = append(pairs, Pair{k, v})
	}

	// Sort the slice based on the values
	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].value < pairs[j].value
	})
	var res []string
	// Find the three keys with the lowest values
	for i := 0; i < 3 && i < len(pairs); i++ {
		res = append(res, pairs[i].key)
	}
	return res
}


func NewGetLocationResp(errorCode int32, primaryIP string, chunckHandle string) *pb.GetLocationResp {
	return &pb.GetLocationResp{Status: &pb.Status{StatusCode: errorCode, ErrorMessage: ErrorCodeToString(errorCode)}, PrimaryIP: primaryIP, ChunkHandle: chunckHandle}
}

func NewCreateResp(errorCode int32) *pb.CreateResp {
	return &pb.CreateResp{Status: &pb.Status{StatusCode: errorCode, ErrorMessage: ErrorCodeToString(errorCode)}}
}