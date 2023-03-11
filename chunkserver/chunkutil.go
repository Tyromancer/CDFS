package chunkserver

import (
	"fmt"
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

	// Address of non-primary chunk servers that holds replicas of this chunk
	PeerAddress []string
}

// LoadChunk reads and returns the chunk file as a whole
func LoadChunk(path string) ([]byte, error) {
	fileContent, err := os.ReadFile(path)
	return fileContent, err
}

// CreateFile attempts to create a file that represents a chunk in the chunk server's local file system
func CreateFile(path string) error {

	err := os.MkdirAll(filepath.Dir(path), 0700)
	if err != nil {
		return err
	}

	_, err = os.Create(path)
	return err
}
