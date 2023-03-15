package chunkserver

import (
	"fmt"
	"github.com/tyromancer/cdfs/pb"
	"log"
	"os"
	"path/filepath"
)

const (
	Primary = iota
	Secondary
)

const (
	OK int32 = iota
	ERROR_NOT_PRIMARY
	ERROR_READ_FAILED
	ERROR_CHUNK_ALREADY_EXISTS
	ERROR_CREATE_CHUNK_FAILED
	ERROR_APPEND_FAILED

	// ERROR_APPEND_NOT_EXISTS represents the chunk to be appended does not exist on local filesystem
	ERROR_APPEND_NOT_EXISTS
)

func ErrorCodeToString(e int32) string {
	switch e {
	case OK:
		return "OK"
	case ERROR_NOT_PRIMARY:
		return "Error: this chunk server is not primary for this chunk"
	case ERROR_READ_FAILED:
		return "Error: failed to open local file for this chunk"
	case ERROR_CHUNK_ALREADY_EXISTS:
		return "Error: chunk already exists"
	case ERROR_APPEND_FAILED:
		return "Error: append to chunk failed"
	case ERROR_APPEND_NOT_EXISTS:
		return "Error: append to chunk not exisis"
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
	Used uint
}

type RespMetaData struct {
	// client last seq
	LastSeq uint32

	// last response to client append request
	AppendResp *pb.AppendDataResp

	// error
	Err error
}

func LoadChunk(path string) ([]byte, error) {
	fileContent, err := os.ReadFile(path)
	return fileContent, err
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

func WriteFile(path string, content []byte) error {
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
	return err
}

// NewReadResp returns a pointer to pb.ReadResp that represents the result of a read with pb.Status
func NewReadResp(seqNum uint32, fileData []byte, errorCode int32) *pb.ReadResp {
	return &pb.ReadResp{SeqNum: seqNum, FileData: fileData, Status: &pb.Status{StatusCode: errorCode, ErrorMessage: ErrorCodeToString(errorCode)}}
}

func NewCreateChunkResp(errorCode int32) *pb.CreateChunkResp {
	return &pb.CreateChunkResp{Status: &pb.Status{StatusCode: errorCode, ErrorMessage: ErrorCodeToString(errorCode)}}
}

func NewAppendDataResp(errorCode int32) *pb.AppendDataResp {
	return &pb.AppendDataResp{Status: &pb.Status{StatusCode: errorCode, ErrorMessage: ErrorCodeToString(errorCode)}}
}
