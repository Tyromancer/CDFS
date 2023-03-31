package masterserver

import (
	"fmt"
	"sort"
	"crypto/rand"
    "encoding/base64"

	"github.com/tyromancer/cdfs/pb"
)

const (
	OK int32 = iota
	ERROR_FILE_NOT_EXISTS
	ERROR_PRIMARY_NOT_EXISTS
	ERROR_FILE_ALREADY_EXISTS
	ERROR_NO_SERVER_AVAILABLE
	ERROR_CHUNKSERVER_ALREADY_EXISTS
	ERROR_FAIL_TO_GENERATE_UNIQUE_TOKEN
)

const (
	ChunkSize = uint32(67108864)
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
	case ERROR_CHUNKSERVER_ALREADY_EXISTS:
		return "Error: the chunk server already exists"
	case ERROR_FAIL_TO_GENERATE_UNIQUE_TOKEN:
		return "Error: fail to generate unique token string"
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

type ClientInfo struct {
	Token string
	UUID string

	// TODO: Save previous response
	GetTokenResp *pb.GetTokenResp

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


// given startOffset and fileHandles, return [index of the start chunk, read offset of start chunk]
func startLocation(fileHandles []HandleMetaData, startOffset uint32) []uint32 {
	var curSize uint = 0
	i := 0
	for curSize + fileHandles[i].Used < uint(startOffset) {
		curSize += fileHandles[i].Used
		i++
	}
	start := startOffset - uint32(curSize)
	return []uint32{uint32(i), start}
}



// given endOffset and fileHandles, return [index of the last chunk, end offset of last chunk]
func endtLocation(fileHandles []HandleMetaData, endOffset uint32) []uint32 {
	var curSize uint = 0
	i := 0
	for curSize + fileHandles[i].Used < uint(endOffset) {
		curSize += fileHandles[i].Used
		i++
	}
	end := endOffset - uint32(curSize)
	return []uint32{uint32(i), end}
}



/* 
Given the length and generate unique token. 
For e.g. given 16 would generate a string token of length 24. 
*/ 
func GenerateToken(length int) (string, error) {
    // generate random bytes
    bytes := make([]byte, length)
    if _, err := rand.Read(bytes); err != nil {
        return "", err
    }

    // encode as base64
    token := base64.StdEncoding.EncodeToString(bytes)

    return token, nil
}



func NewCSRegisterResp(errorCode int32) *pb.CSRegisterResp {
	return &pb.CSRegisterResp{Status: &pb.Status{StatusCode: errorCode, ErrorMessage: ErrorCodeToString(errorCode)}}
}

func NewGetLocationResp(errorCode int32, chunkInfo []*pb.ChunkServerInfo, start uint32, end uint32) *pb.GetLocationResp {
	return &pb.GetLocationResp{Status: &pb.Status{StatusCode: errorCode, ErrorMessage: ErrorCodeToString(errorCode)}, ChunkInfo: chunkInfo, Start: start, End: end}
}

func NewCreateResp(errorCode int32) *pb.CreateResp {
	return &pb.CreateResp{Status: &pb.Status{StatusCode: errorCode, ErrorMessage: ErrorCodeToString(errorCode)}}
}

func NewAppendFileResp(errorCode int32, primaryIP []string, chunckHandle []string) *pb.AppendFileResp {
	return &pb.AppendFileResp{Status: &pb.Status{StatusCode: errorCode, ErrorMessage: ErrorCodeToString(errorCode)}, PrimaryIP: primaryIP, ChunkHandle: chunckHandle}
}

func NewGetTokenResp(uniqueToken string) *pb.GetTokenResp {
	return &pb.GetTokenResp{UniqueToken: uniqueToken}
}