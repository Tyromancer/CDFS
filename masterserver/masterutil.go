package masterserver

import (
	"context"
	"crypto/rand"
	"encoding/base32"
	"errors"
	"fmt"
	"log"
	"sort"

	"github.com/tyromancer/cdfs/pb"
	"google.golang.org/grpc"
)

const (
	OK int32 = iota
	ERROR_FILE_NOT_EXISTS
	ERROR_PRIMARY_NOT_EXISTS
	ERROR_FILE_ALREADY_EXISTS
	ERROR_NO_SERVER_AVAILABLE
	ERROR_CHUNKSERVER_ALREADY_EXISTS
	ERROR_FAIL_TO_GENERATE_UNIQUE_TOKEN
	ERROR_FAIL_TO_DELETE
	ERROR_FAIL_TO_CONNECT_TO_CHUNKSERVER
	ERROR_FAIL_TO_CREATE_CHUNK_WHEN_CREATEFILE
	ERROR_FAIL_TO_CREATE_CHUNK_WHEN_APPEND
	ERROR_DEAD_BECOME_ALIVE
	ERROR_READ_WRONG_OFFSET
	ERROR_READ_WRONG_SIZE
)

const (
	DB_SET = iota
	DB_DELETE
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
		return "Error: the given FileName already exist"
	case ERROR_CHUNKSERVER_ALREADY_EXISTS:
		return "Error: the chunk server already exists"
	case ERROR_FAIL_TO_GENERATE_UNIQUE_TOKEN:
		return "Error: fail to generate unique token string"
	case ERROR_FAIL_TO_DELETE:
		return "Error: fail to delete"
	case ERROR_FAIL_TO_CONNECT_TO_CHUNKSERVER:
		return "Error: fail to dial to chunk server"
	case ERROR_FAIL_TO_CREATE_CHUNK_WHEN_CREATEFILE:
		return "Error: encounter error when creating a chunk during create file"
	case ERROR_FAIL_TO_CREATE_CHUNK_WHEN_APPEND:
		return "Error: encounter error when creating a chunk during append"
	case ERROR_DEAD_BECOME_ALIVE:
		return "Error: dead chunkserver revive"
	case ERROR_READ_WRONG_OFFSET:
		return "Error: read the wrong offset"
	case ERROR_READ_WRONG_SIZE:
		return "Error: invalid read size"
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
	UUID  string

	// TODO: Save previous response
	GetTokenResp *pb.GetTokenResp
}

type ChunkServerInfo struct {
	ChunkHandle []string
	Used        []uint32
	Name        string
}

type ChunkServerChan struct {
	isDead  bool
	channel chan ChunkServerInfo
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

// return the three(or less) chunkservers that have the lowest load given the ChunkServerLoad map
func (s *MasterServer) lowestAllChunkServer(chunkHandle string) []string {
	var pairs []Pair
	for k, v := range s.ChunkServerLoad {
		handleMetaData := s.HandleToMeta[chunkHandle]
		hasChunk := false
		for _, each := range s.CSToHandle[k] {
			if each == handleMetaData {
				//pairs = append(pairs, Pair{k, v})
				hasChunk = true
				break
			}
		}
		if !hasChunk {
			pairs = append(pairs, Pair{k, v})
		}
	}

	// Sort the slice based on the values
	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].value < pairs[j].value
	})
	var res []string
	// Find the all keys with the lowest values
	for i := 0; i < len(pairs); i++ {
		res = append(res, pairs[i].key)
	}
	return res
}

// given startOffset and fileHandles, return [index of the start chunk, read offset of start chunk]
func startLocation(fileHandles []*HandleMetaData, startOffset uint32) (uint32, uint32, error) {
	var curSize uint = 0
	i := 0
	for curSize+fileHandles[i].Used < uint(startOffset) {
		curSize += fileHandles[i].Used
		i++
		if i >= len(fileHandles) {
			return 0, 0, errors.New("invalid start offset")
		}
	}
	start := startOffset - uint32(curSize)
	return uint32(i), start, nil
}

// given endOffset and fileHandles, return [index of the last chunk, end offset of last chunk]
func endLocation(fileHandles []*HandleMetaData, endOffset uint32) (uint32, uint32, error) {
	if endOffset != 0 {
		var curSize uint = 0
		i := 0
		for curSize+fileHandles[i].Used < uint(endOffset) {
			curSize += fileHandles[i].Used
			i++
			if i >= len(fileHandles) {
				return 0, 0, errors.New("Read offset invalid")
			}

		}
		end := endOffset - uint32(curSize)
		return uint32(i), end, nil
	} else {
		lastChunkIndex := len(fileHandles) - 1
		lastChunkUsed := fileHandles[lastChunkIndex].Used
		return uint32(lastChunkIndex), uint32(lastChunkUsed), nil
	}

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
	token := base32.StdEncoding.EncodeToString(bytes)

	return token, nil
}

/*
helper function: given primary and chunkHandle,
call DeleteChunk grpc to delete the chunk
*/
func DeleteChunkHandle(primary string, chunkHandle string) error {
	// Call primary chunk server to delete the chunk with the chunk handle
	var conn *grpc.ClientConn
	conn, err := grpc.Dial(primary, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect to Chunk Server: %v", err)
	}
	defer conn.Close()

	c := pb.NewChunkServerClient(conn)
	req := &pb.DeleteChunkReq{
		ChunkHandle: chunkHandle,
	}
	resp, err := c.DeleteChunk(context.Background(), req)

	// if grpc return an Error, handle this error back in main function
	if resp.GetStatus().StatusCode != OK || err != nil {
		return err
	}
	return nil
}

func checkVersion(backup []string, handle string) (string, error) {
	if len(backup) == 0 {
		return "", errors.New("no backup error")
	}
	version := -1
	resIp := ""
	for _, ip := range backup {
		curVersion, err := readVersion(ip, handle)
		if err != nil {
			// Note: If readVersion resp err, keep check next one?
			if version >= 0 {
				continue
			}
		} else if version < int(curVersion) {
			version = int(curVersion)
			resIp = ip
		}
	}
	if version == -1 {
		return "", errors.New("no backup error")
	}
	return resIp, nil
}

func readVersion(Ip string, handle string) (uint32, error) {
	ctx := context.Background()
	var csConn *grpc.ClientConn
	csConn, err := grpc.Dial(Ip, grpc.WithInsecure())
	defer csConn.Close()
	if err != nil {
		log.Fatalf("Failed to connect to chunk server: %+v", Ip)
		return 0, err
	}
	csClient := pb.NewChunkServerClient(csConn)
	req := pb.ReadVersionReq{
		ChunkHandle: handle,
	}
	res, err := csClient.ReadVersion(ctx, &req)
	if err != nil {
		return 0, err
	}
	if res.GetStatus().StatusCode != 0 {
		return 0, errors.New(res.GetStatus().GetErrorMessage())
	}
	return res.GetVersion(), nil
}

func min(a int, b int) int {
	if a <= b {
		return a
	} else {
		return b
	}
}

func NewCSRegisterResp(errorCode int32) *pb.CSRegisterResp {
	return &pb.CSRegisterResp{
		Status: &pb.Status{StatusCode: errorCode, ErrorMessage: ErrorCodeToString(errorCode)},
	}
}

func NewGetLocationResp(errorCode int32, chunkInfo []*pb.ChunkServerInfo, start uint32, end uint32) *pb.GetLocationResp {
	return &pb.GetLocationResp{
		Status:    &pb.Status{StatusCode: errorCode, ErrorMessage: ErrorCodeToString(errorCode)},
		ChunkInfo: chunkInfo,
		Start:     start,
		End:       end,
	}
}

func NewCreateResp(errorCode int32) *pb.CreateResp {
	return &pb.CreateResp{
		Status: &pb.Status{StatusCode: errorCode, ErrorMessage: ErrorCodeToString(errorCode)},
	}
}

func NewAppendFileResp(errorCode int32, primaryIP []string, chunckHandle []string) *pb.AppendFileResp {
	return &pb.AppendFileResp{
		Status:      &pb.Status{StatusCode: errorCode, ErrorMessage: ErrorCodeToString(errorCode)},
		PrimaryIP:   primaryIP,
		ChunkHandle: chunckHandle,
	}
}

func NewGetTokenResp(uniqueToken string) *pb.GetTokenResp {
	return &pb.GetTokenResp{
		UniqueToken: uniqueToken,
	}
}

func NewDeleteStatus(errorCode int32) *pb.DeleteStatus {
	return &pb.DeleteStatus{
		Status: &pb.Status{StatusCode: errorCode, ErrorMessage: ErrorCodeToString(errorCode)},
	}
}

func NewAppendResultResp() *pb.AppendResultResp {
	return &pb.AppendResultResp{}
}

func NewHeartBeatResp(errorCode int32) *pb.HeartBeatResp {
	return &pb.HeartBeatResp{
		Status: &pb.Status{StatusCode: errorCode, ErrorMessage: ErrorCodeToString(errorCode)},
	}
}
