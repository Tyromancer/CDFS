package chunkserver

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/tyromancer/cdfs/pb"
	"google.golang.org/grpc"
	"net"
	"os"
	"path"
	"testing"
)

func NewChunkServerInstance(t *testing.T, port uint32, msHost string, msPort uint32, basePath string) {
	t.Log("Create New Chunk Server")
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		t.Fatalf("Failed to listen on port %d %v", port, err)
	}

	s := ChunkServer{Chunks: make(map[string]*ChunkMetaData), ClientLastResp: make(map[string]RespMetaData), ServerName: fmt.Sprintf("localhost:%d", port), BasePath: basePath, HostName: "localhost", Port: port, MasterIP: msHost, MasterPort: msPort}

	if msHost != "" {
		err = s.SendRegister()
		if err != nil {
			t.Fatalf("Failed to register on MS: %v", err)
		}
	}

	grpcServer := grpc.NewServer()
	pb.RegisterChunkServerServer(grpcServer, &s)

	if err = grpcServer.Serve(lis); err != nil {
		t.Fatalf("Failed to serve chunk server: %v", err)
	}
}

func newChunkServer(t *testing.T, name string) ChunkServer {
	return ChunkServer{Chunks: make(map[string]*ChunkMetaData), ClientLastResp: make(map[string]RespMetaData), ServerName: name, BasePath: t.TempDir()}
}

func setChunkMetaData(cs *ChunkServer, chunkHandle string, chunkLocation string, role uint32, primary string) {
	cs.Chunks[chunkHandle] = &ChunkMetaData{ChunkLocation: chunkLocation, Role: role, PrimaryChunkServer: primary}
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

func appendChunkWorkload(t *testing.T, server *ChunkServer, chunkHandle string, appendContent []byte, uuid string) error {
	req := pb.AppendDataReq{
		ChunkHandle: chunkHandle,
		FileData:    appendContent,
		Token:       "appendChunk#1",
		Uuid:        uuid,
	}
	got, err := server.AppendData(context.Background(), &req)
	if got.GetStatus().GetStatusCode() != OK || err != nil {
		t.Logf("got status code %d, expected %d, message is %s", got.GetStatus().GetStatusCode(), OK, got.GetStatus().GetErrorMessage())
		return errors.New(got.GetStatus().GetErrorMessage())
	}
	return nil
}

func TestClientReadInvalidRead(t *testing.T) {
	t.Log("Running TestClientReadInvalidRead...")

	s := newChunkServer(t, "cs1")
	setChunkMetaData(&s, "chunk#1", "/cdfs/cs1/chunk1", Secondary, "cs2")

	req1 := pb.ReadReq{ChunkHandle: "chunk#2", Token: "client1"}
	got1, err := s.Read(context.Background(), &req1)
	wanted1 := ERROR_NOT_PRIMARY
	if got1.Status.GetStatusCode() != wanted1 || err == nil {
		t.Errorf("got status code %d, expected %d, returned error is %v", got1.Status.GetStatusCode(), wanted1, err)
	}
}

// TestCreateValidChunk tests the ChunkServer behavior on creating a valid chunk via the Create RPC
func TestCreateValidChunk(t *testing.T) {
	t.Log("Running TestCreateValidChunk...")
	primaryAddr, primaryPort := "localhost", uint32(12345)
	backupAddr1, backupPort1 := "localhost", uint32(12346)
	backupAddr2, backupPort2 := "localhost", uint32(12347)

	primaryBasePath, backupBasePath1, backupBasePath2 := t.TempDir(), t.TempDir(), t.TempDir()

	go NewChunkServerInstance(t, primaryPort, "", 0, primaryBasePath)
	go NewChunkServerInstance(t, backupPort1, "", 0, backupBasePath1)
	go NewChunkServerInstance(t, backupPort2, "", 0, backupBasePath2)

	// send create rpc to primary
	chunkHandle := "chunk#1"
	role := Primary

	primaryConn := NewPeerConn()

	chunkFilePath, err := createChunkWorkload(t, &primary, "chunk#1", nil)
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
	t.Log("Running TestValidAppend...")
	chunkHandle := "chunk#1"
	s := newChunkServer(t, "cs1")
	chunkFilePath1, err := createChunkWorkload(t, &s, chunkHandle, nil)
	t.Log("chunk file path is ", chunkFilePath1)
	if err != nil {
		t.Errorf("got error %v", err)
	}
	appendContent := []byte("appendDataChunkTest")
	err = appendChunkWorkload(t, &s, chunkHandle, 1, appendContent)
	if err != nil {
		t.Errorf("TestValidAppend failed, got error %v", err)
	}
	result, err := os.ReadFile(chunkFilePath1)
	if err != nil {
		t.Errorf("TestValidAppend failed, got error %v", err)
	}

	if !bytes.Equal(result, appendContent) {
		t.Errorf("TestValidAppend failed, the results do not match")
	}
}

func TestAppendToNonExistingChunk(t *testing.T) {
	t.Log("Running TestAppendToNonExistingChunk...")
	chunkHandle := "chunk#1"
	s := newChunkServer(t, "cs1")

	appendContent := []byte("appendDataChunkTest")
	err := appendChunkWorkload(t, &s, chunkHandle, 1, appendContent)
	if err == nil {
		t.Errorf("TestAppendToNonExistingChunk failed, should get err")
	}
}

func TestAppendWithPrevSeqNum(t *testing.T) {
	t.Log("Running TestAppendWithPrevSeqNum...")
	chunkHandle := "chunk#1"
	s := newChunkServer(t, "cs1")
	chunkFilePath1, err := createChunkWorkload(t, &s, chunkHandle, nil)
	t.Log("chunk file path is ", chunkFilePath1)
	if err != nil {
		t.Errorf("got error %v", err)
	}
	appendContent := []byte("appendDataChunkTest")
	err = appendChunkWorkload(t, &s, chunkHandle, 1, appendContent)
	err = appendChunkWorkload(t, &s, chunkHandle, 1, appendContent)
	if err != nil {
		t.Errorf("TestAppendWithPrevSeqNum failed, got error %v", err)
	}
	err = appendChunkWorkload(t, &s, chunkHandle, 0, appendContent)
	if err == nil {
		t.Errorf("TestAppendWithPrevSeqNum failed, should have error")
	}
}
