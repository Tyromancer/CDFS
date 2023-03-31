package chunkserver

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/tyromancer/cdfs/pb"
	"golang.org/x/exp/constraints"
	"google.golang.org/grpc"
)

const (
	Primary = iota
	Secondary
)

const (
	OK int32 = iota
	ERROR_NOT_PRIMARY
	ERROR_NOT_SECONDARY
	ERROR_READ_FAILED
	ERROR_CHUNK_ALREADY_EXISTS
	ERROR_CREATE_CHUNK_FAILED
	ERROR_APPEND_FAILED
	ERROR_REPLICATE_FAILED
	// ERROR_APPEND_NOT_EXISTS represents the chunk to be appended does not exist on local filesystem
	ERROR_APPEND_NOT_EXISTS
	ERROR_REPLICATE_NOT_EXISTS
	ERROR_SHOULD_NOT_HAPPEN
	ERROR_CHUNK_NOT_EXISTS
	ERROR_VERSIONS_DO_NOT_MATCH
)

func ErrorCodeToString(e int32) string {
	switch e {
	case OK:
		return "OK"
	case ERROR_NOT_PRIMARY:
		return "Error: this chunk server is not primary for this chunk"
	case ERROR_NOT_SECONDARY:
		return "Error: this chunk server is not backup for this chunk"
	case ERROR_READ_FAILED:
		return "Error: failed to open local file for this chunk"
	case ERROR_CHUNK_ALREADY_EXISTS:
		return "Error: chunk already exists"
	case ERROR_APPEND_FAILED:
		return "Error: append to chunk failed"
	case ERROR_REPLICATE_FAILED:
		return "Error: replicate to chunk failed"
	case ERROR_APPEND_NOT_EXISTS:
		return "Error: append to chunk not exists"
	case ERROR_REPLICATE_NOT_EXISTS:
		return "Error: replicate chunk not exists"
	case ERROR_SHOULD_NOT_HAPPEN:
		return "Error: this should not have happened"
	case ERROR_CHUNK_NOT_EXISTS:
		return "Error: chunk does not exist on this server"
	case ERROR_VERSIONS_DO_NOT_MATCH:
		return "Error: chunk versions do not match"

	default:
		return fmt.Sprintf("%d", int(e))
	}
}

type ChunkMetaData struct {
	// file location in local file system
	ChunkLocation string

	// role of current chunkserver for this chunk
	Role uint32

	// IP address of primary chunk server for this chunk
	PrimaryChunkServer string
	PeerAddress        []string

	// Already used size in bytes
	Used uint32

	// Add version number
	Version uint32

	MetaDataLock sync.Mutex

	GetVersionChannel chan string
}

type RespMetaData struct {
	// client last seq
	LastID string

	// last response to client append request
	AppendResp *pb.AppendDataResp

	// error
	Err error
}

// LoadChunk reads a file at the specified path with an offset start and ends the read at end
// if end equals to 0, LoadChunk reads and returns the whole data starting from start, otherwise
// it reads and returns (end - start) bytes
func LoadChunk(path string, start uint32, end uint32) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	buffer := make([]byte, 0)
	_, err = file.ReadAt(buffer, int64(start))
	if err != nil {
		return nil, err
	}

	if end == 0 {
		return buffer, nil
	}

	return buffer[:end-start], nil
}

func CreateFile(path string) error {

	err := os.MkdirAll(filepath.Dir(path), 0700)
	if err != nil {
		return err
	}

	f, err := os.Create(path)
	defer f.Close()
	return err
}

func OverWriteChunk(chunkMeta *ChunkMetaData, content []byte) error {
	path := chunkMeta.ChunkLocation
	f, err := os.OpenFile(path, os.O_WRONLY, 0600)
	if err != nil {
		return err
	}

	defer func(f *os.File) {
		err := f.Close()
		if err != nil {
			log.Println("failed to close file: ", err)
		}
	}(f)

	_, err = f.Write(content)
	if err != nil {
		return err
	}
	chunkMeta.Used = uint32(len(content))
	return nil
}

func WriteFile(chunkMeta *ChunkMetaData, content []byte) error {
	path := chunkMeta.ChunkLocation
	chunkMeta.MetaDataLock.Lock()
	defer chunkMeta.MetaDataLock.Unlock()

	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}

	defer func(f *os.File) {
		err := f.Close()
		if err != nil {
			log.Println("failed to close file: ", err)
		}
	}(f)

	_, err = f.Write(content)
	if err != nil {
		return err
	}
	chunkMeta.Version++
	chunkMeta.Used += uint32(len(content))
	return nil
}

func NewStatus(errorCode int32) *pb.Status {
	return &pb.Status{
		StatusCode:   errorCode,
		ErrorMessage: ErrorCodeToString(errorCode),
	}
}

// NewReadResp returns a pointer to pb.ReadResp that represents the result of a read with pb.Status
func NewReadResp(fileData []byte, errorCode int32, version *uint32) *pb.ReadResp {
	return &pb.ReadResp{FileData: fileData, Status: NewStatus(errorCode), Version: version}
}

func NewCreateChunkResp(errorCode int32) *pb.CreateChunkResp {
	return &pb.CreateChunkResp{Status: NewStatus(errorCode)}
}

func NewForwardCreateResp(errorCode int32) *pb.ForwardCreateResp {
	return &pb.ForwardCreateResp{Status: NewStatus(errorCode)}
}

func NewAppendDataResp(errorCode int32) *pb.AppendDataResp {
	return &pb.AppendDataResp{Status: NewStatus(errorCode)}
}

func NewReplicateResp(errorCode int32, uuid string) *pb.ReplicateResp {
	return &pb.ReplicateResp{Status: NewStatus(errorCode), Uuid: uuid}
}

func NewDeleteChunkResp(errorCode int32) *pb.DeleteChunkResp {
	return &pb.DeleteChunkResp{Status: NewStatus(errorCode)}
}

func NewReadVersionResp(errorCode int32, version *uint32) *pb.ReadVersionResp {
	return &pb.ReadVersionResp{
		Status:  NewStatus(errorCode),
		Version: version,
	}
}

func NewGetVersionResp(errorCode int32, version *uint32, fileData []byte) *pb.GetVersionResp {
	return &pb.GetVersionResp{
		Status:    NewStatus(errorCode),
		Version:   version,
		ChunkData: fileData,
	}
}

// NewPeerConn establishes and returns a grpc.ClientConn to the specified address
func NewPeerConn(address string) (*grpc.ClientConn, error) {
	var conn *grpc.ClientConn
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	return conn, err
}

// ForwardCreateReq establishes a grpc.ClientConn with peer and forwards the pb.CreateChunkReq
func ForwardCreateReq(req *pb.CreateChunkReq, peer string) error {
	peerConn, err := NewPeerConn(peer)

	if err != nil {
		return err
	}

	defer peerConn.Close()

	peerClient := pb.NewChunkServerClient(peerConn)
	forwardReq := &pb.ForwardCreateReq{
		ChunkHandle: req.ChunkHandle,
		Primary:     req.Primary,
	}

	res, err := peerClient.ForwardCreate(context.Background(), forwardReq)

	// NOTE: if err != nil, will res be nil?
	if err != nil || res.GetStatus().GetStatusCode() != OK {
		return errors.New(res.GetStatus().GetErrorMessage())
	}

	return nil
}

func NewReplicateReq(req *pb.ReplicateReq, peer string) error {
	peerConn, err := NewPeerConn(peer)

	if err != nil {
		return err
	}
	defer peerConn.Close()

	peerClient := pb.NewChunkServerClient(peerConn)
	res, err := peerClient.Replicate(context.Background(), req)
	if err != nil || res.GetStatus().GetStatusCode() != OK {
		return errors.New(res.GetStatus().GetErrorMessage())
	}
	return nil
}

func ReplicateRespToAppendResp(replicateResp *pb.ReplicateResp) *pb.AppendDataResp {
	return &pb.AppendDataResp{Status: replicateResp.GetStatus()}
}

func IsClose[T any](ch <-chan T) bool {
	select {
	case <-ch:
		return true
	default:
	}
	return false
}

type Number interface {
	constraints.Integer | constraints.Float
}

func Sum[T Number](slice []T) T {
	if len(slice) == 0 {
		return 0
	}
	result := slice[0]
	if len(slice) == 1 {
		return result
	}
	for _, v := range slice[1:] {
		result += v
	}
	return result
}
