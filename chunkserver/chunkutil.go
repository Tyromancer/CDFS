package chunkserver

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/tyromancer/cdfs/pb"
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
	Role uint

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

	_, err = os.Create(path)
	return err
}

func WriteFile(path string, content []byte) error {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}

	defer f.Close()

	_, err = f.Write(content)
	return err
}
