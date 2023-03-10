package chunkserver

import (
	"fmt"
	"os"
)

const (
	Primary = iota
	Secondary
)

const (
	ERROR_NOT_PRIMARY int32 = iota
	ERROR_READ_FAILED
)

func ErrorCodeToString(e int32) string {
	switch e {
	case ERROR_NOT_PRIMARY:
		return "Error: this chunk server is not primary for this chunk"
	case ERROR_READ_FAILED:
		return "Error: failed to open local file for this chunk"
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
}

func LoadChunk(location string) ([]byte, error) {
	fileContent, err := os.ReadFile(location)
	return fileContent, err
}
