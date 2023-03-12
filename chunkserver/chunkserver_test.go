package chunkserver

import (
	"context"
	"errors"
	"github.com/tyromancer/cdfs/pb"
	"os"
	"path"
	"testing"
)

func newChunkServer(t *testing.T, name string) ChunkServer {
	return ChunkServer{Chunks: make(map[string]ChunkMetaData), ClientLastResp: make(map[string]RespMetaData), ServerName: name, BasePath: t.TempDir()}
}

func setChunkMetaData(cs *ChunkServer, chunkHandle string, chunkLocation string, role uint, primary string) {
	cs.Chunks[chunkHandle] = ChunkMetaData{ChunkLocation: chunkLocation, Role: role, PrimaryChunkServer: primary}
}

func makeFilePath(server *ChunkServer, chunkHandle string) string {
	return path.Join(server.BasePath, chunkHandle)
}

func createChunkWorkload(t *testing.T, server *ChunkServer, chunkHandle string, peers []string) (string, error) {
	req := pb.CreateChunkReq{
		ChunkHandle: chunkHandle,
		Role:        Primary,
		Peers:       peers,
	}

	got, err := server.CreateChunk(context.Background(), &req)
	chunkFilePath := makeFilePath(server, chunkHandle)
	if got.GetStatus().GetStatusCode() != OK || err != nil {
		t.Logf("got status code %d, expected %d, message is %s", got.GetStatus().GetStatusCode(), OK, got.GetStatus().GetErrorMessage())
		return chunkFilePath, errors.New(got.GetStatus().GetErrorMessage())
	}

	return chunkFilePath, nil
}

// checkFileExists checks if the file specified by path exists and is a file rather than a directory
//
//	reference: https://stackoverflow.com/questions/12518876/how-to-check-if-a-file-exists-in-go
func checkFileExists(path string) bool {
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false
	}
	return !info.IsDir()
}

func TestClientReadInvalidRead(t *testing.T) {
	t.Log("Running TestClientReadInvalidRead...")

	s := newChunkServer(t, "cs1")
	setChunkMetaData(&s, "chunk#1", "/cdfs/cs1/chunk1", Secondary, "cs2")

	req1 := pb.ReadReq{SeqNum: 0, ChunkHandle: "chunk#2", Token: "client1"}
	got1, err := s.Read(context.Background(), &req1)
	wanted1 := ERROR_NOT_PRIMARY
	if got1.Status.GetStatusCode() != wanted1 || err == nil {
		t.Errorf("got status code %d, expected %d, returned error is %v", got1.Status.GetStatusCode(), wanted1, err)
	}
}

// TestCreateValidChunk tests the ChunkServer behavior on creating a valid chunk via the Create RPC
func TestCreateValidChunk(t *testing.T) {
	t.Log("Running TestCreateValidChunk...")
	s := newChunkServer(t, "cs1")
	chunkFilePath, err := createChunkWorkload(t, &s, "chunk#1", nil)
	if err != nil {
		t.Errorf("got error %v", err)
	}

	exists := checkFileExists(chunkFilePath)
	if !exists {
		t.Errorf("chunk file does not exist at %s", chunkFilePath)
	}
}

func TestCreateDuplicateChunk(t *testing.T) {
	t.Log("Running TestCreateDuplicateChunk...")
	chunkHandle := "chunk#1"
	s := newChunkServer(t, "cs1")
	chunkFilePath1, err := createChunkWorkload(t, &s, chunkHandle, nil)
	t.Log("chunk file path is ", chunkFilePath1)
	if err != nil {
		t.Errorf("got error %v", err)
	}

	exists := checkFileExists(chunkFilePath1)
	if !exists {
		t.Errorf("chunk file does not exist at %s", chunkFilePath1)
	}

	_, err = createChunkWorkload(t, &s, chunkHandle, nil)
	if err == nil {
		t.Errorf("did not get expected error")
	}
}

func TestValidAppend(t *testing.T) {

}

func TestAppendToNonExistingChunk(t *testing.T) {

}

func TestAppendWithPrevSeqNum(t *testing.T) {

}
