package chunkserver

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/tyromancer/cdfs/pb"
	"google.golang.org/grpc"
	"net"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

type BuildChunkServerConfig struct {
	PrimaryPort uint32
	Backup1Port uint32
	Backup2Port uint32

	MasterHost string
	MasterPort uint32
}

type WithStatus interface {
	GetStatus() *pb.Status
}

func CheckError(t *testing.T, msg WithStatus, err error, expected int32) {
	if err != nil {
		t.Errorf("connection error %v", err)
	}

	errorCode := msg.GetStatus().GetStatusCode()
	if errorCode != expected {
		t.Errorf("expected %s, got %s", ErrorCodeToString(expected), msg.GetStatus().GetErrorMessage())
	}
}

func BuildAndRunThreeChunkServers(t *testing.T, config *BuildChunkServerConfig) (p *ChunkServer, b1 *ChunkServer, b2 *ChunkServer, debugChan chan DebugInfo) {
	primaryPort := config.PrimaryPort
	backupPort1 := config.Backup1Port
	backupPort2 := config.Backup2Port

	primaryBasePath, backupBasePath1, backupBasePath2 := t.TempDir(), t.TempDir(), t.TempDir()

	debugChan = make(chan DebugInfo)
	p = buildChunkServer(t, primaryPort, config.MasterHost, config.MasterPort, primaryBasePath, debugChan)
	b1 = buildChunkServer(t, backupPort1, config.MasterHost, config.MasterPort, backupBasePath1, debugChan)
	b2 = buildChunkServer(t, backupPort2, config.MasterHost, config.MasterPort, backupBasePath2, debugChan)

	go startChunkServer(t, p)
	go startChunkServer(t, b1)
	go startChunkServer(t, b2)
	return
}

func startChunkServer(t *testing.T, cs *ChunkServer) {
	t.Log("Start Chunk Server")
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", cs.Port))
	if err != nil {
		t.Fatalf("Failed to listen on port %d %v", cs.Port, err)
	}

	if cs.MasterIP != "" {
		err = cs.SendRegister()
		if err != nil {
			t.Fatalf("Failed to register on MS: %v", err)
		}
	}

	grpcServer := grpc.NewServer()
	pb.RegisterChunkServerServer(grpcServer, cs)

	if err = grpcServer.Serve(lis); err != nil {
		t.Fatalf("Failed to serve chunk server: %v", err)
	}
}

func buildChunkServer(t *testing.T, port uint32, msHost string, msPort uint32, basePath string, debugChan chan DebugInfo) *ChunkServer {
	s := &ChunkServer{
		Chunks:         make(map[string]*ChunkMetaData),
		ClientLastResp: make(map[string]RespMetaData),
		ServerName:     fmt.Sprintf("localhost:%d", port),
		BasePath:       basePath,
		HostName:       "localhost",
		Port:           port,
		MasterIP:       msHost,
		MasterPort:     msPort,
		Debug:          true,
		DebugChan:      debugChan,
	}
	return s
}

func newChunkServer(t *testing.T, name string) ChunkServer {
	return ChunkServer{Chunks: make(map[string]*ChunkMetaData), ClientLastResp: make(map[string]RespMetaData), ServerName: name, BasePath: t.TempDir()}
}

func setChunkMetaData(cs *ChunkServer, chunkHandle string, chunkLocation string, role uint32, primary string) {
	cs.Chunks[chunkHandle] = &ChunkMetaData{ChunkLocation: chunkLocation, Role: role, PrimaryChunkServer: primary}
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

// TestCreateValidChunk tests the ChunkServer behavior on creating a valid chunk via the Create RPC
func TestCreateValidChunkWithReplication(t *testing.T) {
	t.Log("Running TestCreateValidChunk...")
	p, b1, b2, debugChan := BuildAndRunThreeChunkServers(t, &BuildChunkServerConfig{
		PrimaryPort: 12345,
		Backup1Port: 12346,
		Backup2Port: 12347,
		MasterHost:  "",
		MasterPort:  0,
	})
	defer close(debugChan)

	time.Sleep(time.Duration(100) * time.Millisecond)

	// send create rpc to primary
	chunkHandle := "chunk#1"
	role := Primary

	primaryConn, err := NewPeerConn(p.ServerName)
	if err != nil {
		t.Errorf("failed to connect to primary goroutine: %v", err)
	}
	defer primaryConn.Close()

	req := &pb.CreateChunkReq{
		ChunkHandle: chunkHandle,
		Role:        uint32(role),
		Primary:     p.ServerName,
		Peers:       []string{b1.ServerName, b2.ServerName},
	}

	primaryCS := pb.NewChunkServerClient(primaryConn)
	res, err := primaryCS.CreateChunk(context.Background(), req)
	if err != nil || res.GetStatus().GetStatusCode() != OK {
		t.Errorf("failed to create chunk: %v", err)
	}

	time.Sleep(time.Duration(100) * time.Millisecond)
	counter := 0
	for counter < 2 {
		select {
		case info := <-debugChan:
			if strings.Compare("GetVersion", info.Func) == 0 && strings.Compare(info.Addr, p.ServerName) == 0 {
				counter += 1
				t.Logf("Got debug info: Addr: %s, Func: %s, StatusCode: %d", info.Addr, info.Func, info.StatusCode)
			} else {
				t.Errorf("Got not expected debug info: Addr: %s, Func: %s, StatusCode: %d", info.Addr, info.Func, info.StatusCode)
			}
		}
	}

	primaryExists, backupExists1, backupExists2 := checkFileExists(p.BasePath+"/"+chunkHandle), checkFileExists(b1.BasePath+"/"+chunkHandle), checkFileExists(b2.BasePath+"/"+chunkHandle)
	if !primaryExists {
		t.Errorf("primary chunk file does not exist")
	} else if !backupExists1 {
		t.Errorf("backup1 chunk file does not exist")
	} else if !backupExists2 {
		t.Errorf("backup2 chunk file does not exist")
	}
}

func TestCreateDuplicateChunk(t *testing.T) {
	t.Log("Running TestCreateDuplicateChunk...")
	primaryHost, primaryPort := "localhost", uint32(12345)
	primaryBasePath := t.TempDir()
	debugChan := make(chan DebugInfo)
	primary := buildChunkServer(t, primaryPort, "", 0, primaryBasePath, debugChan)

	chunkHandle := "chunk#1"
	role := Primary
	req := &pb.CreateChunkReq{
		ChunkHandle: chunkHandle,
		Role:        uint32(role),
		Primary:     GetAddr(primaryHost, primaryPort),
		Peers:       []string{},
	}

	res, err := primary.CreateChunk(context.Background(), req)
	if err != nil || res.GetStatus().GetStatusCode() != OK {
		t.Errorf("failed to create chunk: %v", err)
	}

	res, err = primary.CreateChunk(context.Background(), req)
	if err != nil {
		t.Errorf("connection failure: %v", err)
	}
	if res.GetStatus().GetStatusCode() != ERROR_CHUNK_ALREADY_EXISTS {
		t.Errorf("expected chunk already exists, got: %s", res.GetStatus().GetErrorMessage())
	}
}

func TestAppend(t *testing.T) {
	t.Log("Running TestValidAppend...")
	ctx := context.Background()
	p, b1, b2, debugChan := BuildAndRunThreeChunkServers(t, &BuildChunkServerConfig{
		PrimaryPort: 12345,
		Backup1Port: 12346,
		Backup2Port: 12347,
		MasterHost:  "",
		MasterPort:  0,
	})
	defer close(debugChan)

	time.Sleep(time.Duration(100) * time.Millisecond)

	// send create rpc to primary
	chunkHandle := "chunk#1"
	role := Primary

	primaryConn, err := NewPeerConn(p.ServerName)
	if err != nil {
		t.Errorf("failed to connect to primary goroutine: %v", err)
	}
	defer primaryConn.Close()

	req := &pb.CreateChunkReq{
		ChunkHandle: chunkHandle,
		Role:        uint32(role),
		Primary:     p.ServerName,
		Peers:       []string{b1.ServerName, b2.ServerName},
	}

	primaryCS := pb.NewChunkServerClient(primaryConn)
	res, err := primaryCS.CreateChunk(ctx, req)
	if err != nil || res.GetStatus().GetStatusCode() != OK {
		t.Errorf("failed to create chunk: %v", err)
	}

	appendReqID := uuid.New()
	appendReq := &pb.AppendDataReq{
		ChunkHandle: chunkHandle,
		FileData:    []byte("appendDataChunkTest"),
		Token:       "client#1",
		Uuid:        appendReqID.String(),
	}

	appendRes, err := primaryCS.AppendData(ctx, appendReq)
	if err != nil || appendRes.GetStatus().GetStatusCode() != OK {
		t.Errorf("failed to append to primary CS: %v", err)
	}

	// check chunk version of all three replicas
	primaryVersion := p.Chunks[chunkHandle].Version
	backup1Version := b1.Chunks[chunkHandle].Version
	backup2Version := b2.Chunks[chunkHandle].Version

	t.Logf("primary version: %d, backup1 version: %d, backup2 version: %d", primaryVersion, backup1Version, backup2Version)

	if primaryVersion != backup1Version || primaryVersion != backup2Version || backup1Version != backup2Version {
		t.Errorf("versions differ, got primary %d, backup1 %d, backup2 %d", primaryVersion, backup1Version, backup2Version)
	}

	// append again (mock duplicate request)
	appendRes, err = primaryCS.AppendData(ctx, appendReq)
	if err != nil || appendRes.GetStatus().GetStatusCode() != OK {
		t.Errorf("expected duplicated last successful append operation to return OK, got %s", appendRes.GetStatus().GetErrorMessage())
	}

	dupPrimaryVersion := p.Chunks[chunkHandle].Version
	if dupPrimaryVersion != primaryVersion {
		t.Errorf("duplicate append changed chunk version from %d to %d", primaryVersion, dupPrimaryVersion)
	}

	// append to non-existing chunk
	ctx1, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	uuidNonExistent := uuid.New()
	reqNonExistent := &pb.AppendDataReq{
		ChunkHandle: "chunk#Invalid",
		FileData:    []byte("invalid chunk"),
		Token:       "client#1",
		Uuid:        uuidNonExistent.String(),
	}

	appendRes2, err := primaryCS.AppendData(ctx1, reqNonExistent)
	if err != nil {
		t.Errorf("connection failure: %v", err)
	}
	if appendRes2.GetStatus().GetStatusCode() != ERROR_APPEND_NOT_EXISTS {
		t.Errorf("expected error append not exists, got %s", appendRes2.GetStatus().GetErrorMessage())
	}

	_, ok := p.Chunks["chunk#Invalid"]
	if ok {
		t.Errorf("append to invalid chunk handle unexpectedly recorded by primary chunk server")
	}

	// append to backup
	appendReqID2 := uuid.New()
	appendReq2 := &pb.AppendDataReq{
		ChunkHandle: chunkHandle,
		FileData:    []byte("appendDataChunkTest2"),
		Token:       "client#1",
		Uuid:        appendReqID2.String(),
	}

	backupConn1, err := NewPeerConn(b1.ServerName)
	if err != nil {
		t.Errorf("failed to connect to backup1 goroutine: %v", err)
	}
	defer backupConn1.Close()

	backupCS := pb.NewChunkServerClient(backupConn1)
	backupRes, err := backupCS.AppendData(ctx, appendReq2)
	if err != nil {
		t.Errorf("connection failure")
	}
	if backupRes.GetStatus().GetStatusCode() != ERROR_NOT_PRIMARY {
		t.Errorf("expected error not primary, got %s", backupRes.GetStatus().GetErrorMessage())
	}
}

func TestDelete(t *testing.T) {
	t.Log("Running TestValidAppend...")
	ctx := context.Background()
	p, b1, b2, debugChan := BuildAndRunThreeChunkServers(t, &BuildChunkServerConfig{
		PrimaryPort: 12345,
		Backup1Port: 12346,
		Backup2Port: 12347,
		MasterHost:  "",
		MasterPort:  0,
	})
	defer close(debugChan)

	time.Sleep(time.Duration(100) * time.Millisecond)

	// create a chunk
	chunkHandle := "chunk#1"
	role := Primary

	primaryConn, err := NewPeerConn(p.ServerName)
	if err != nil {
		t.Errorf("failed to connect to primary goroutine: %v", err)
	}

	req := &pb.CreateChunkReq{
		ChunkHandle: chunkHandle,
		Role:        uint32(role),
		Primary:     p.ServerName,
		Peers:       []string{b1.ServerName, b2.ServerName},
	}

	primaryCS := pb.NewChunkServerClient(primaryConn)
	res, err := primaryCS.CreateChunk(context.Background(), req)
	if err != nil || res.GetStatus().GetStatusCode() != OK {
		t.Errorf("failed to create chunk: %v", err)
	}

	// delete chunk from all three chunk servers
	b1Conn, err := NewPeerConn(b1.ServerName)
	if err != nil {
		t.Errorf("failed to connect to backup1 goroutine: %v", err)
	}
	defer b1Conn.Close()

	b2Conn, err := NewPeerConn(b2.ServerName)
	if err != nil {
		t.Errorf("failed to connect to backup2 goroutine: %v", err)
	}
	defer b2Conn.Close()

	b1CS := pb.NewChunkServerClient(b1Conn)
	b2CS := pb.NewChunkServerClient(b2Conn)

	delReq := &pb.DeleteChunkReq{ChunkHandle: chunkHandle}
	deleteRes, err := primaryCS.DeleteChunk(ctx, delReq)
	CheckError(t, deleteRes, err, OK)

	deleteRes1, err := b1CS.DeleteChunk(ctx, delReq)
	CheckError(t, deleteRes1, err, OK)

	deleteRes2, err := b2CS.DeleteChunk(ctx, delReq)
	CheckError(t, deleteRes2, err, OK)

	// check if metadata is recorded in chunk servers
	if _, ok := p.Chunks[chunkHandle]; ok {
		t.Errorf("chunk metadata still in primary")
	}
	if _, ok := b1.Chunks[chunkHandle]; ok {
		t.Errorf("chunk metadata still in backup1")
	}
	if _, ok := b2.Chunks[chunkHandle]; ok {
		t.Errorf("chunk metadata still in backup2")
	}
}

func TestRead(t *testing.T) {
	t.Log("Running TestRead...")
	ctx := context.Background()
	p, b1, b2, debugChan := BuildAndRunThreeChunkServers(t, &BuildChunkServerConfig{
		PrimaryPort: 12345,
		Backup1Port: 12346,
		Backup2Port: 12347,
		MasterHost:  "",
		MasterPort:  0,
	})
	defer close(debugChan)

	time.Sleep(time.Duration(100) * time.Millisecond)

	// create a chunk
	chunkHandle := "chunk#1"
	role := Primary

	primaryConn, err := NewPeerConn(p.ServerName)
	if err != nil {
		t.Errorf("failed to connect to primary goroutine: %v", err)
	}
	defer primaryConn.Close()

	req := &pb.CreateChunkReq{
		ChunkHandle: chunkHandle,
		Role:        uint32(role),
		Primary:     p.ServerName,
		Peers:       []string{b1.ServerName, b2.ServerName},
	}

	primaryCS := pb.NewChunkServerClient(primaryConn)
	res, err := primaryCS.CreateChunk(context.Background(), req)
	if err != nil || res.GetStatus().GetStatusCode() != OK {
		t.Errorf("failed to create chunk: %v", err)
	}

	// append to chunk
	appendReqID := uuid.New()
	appendData := []byte("appendDataChunkTest")
	appendReq := &pb.AppendDataReq{
		ChunkHandle: chunkHandle,
		FileData:    appendData,
		Token:       "client#1",
		Uuid:        appendReqID.String(),
	}

	appendRes, err := primaryCS.AppendData(ctx, appendReq)
	if err != nil || appendRes.GetStatus().GetStatusCode() != OK {
		t.Errorf("failed to append to primary CS: %v", err)
	}

	// read a chunk
	b1Conn, err := NewPeerConn(b1.ServerName)
	if err != nil {
		t.Errorf("failed to connect to backup1 goroutine: %v", err)
	}
	defer b1Conn.Close()

	b2Conn, err := NewPeerConn(b2.ServerName)
	if err != nil {
		t.Errorf("failed to connect to backup2 goroutine: %v", err)
	}
	defer b2Conn.Close()

	b1CS := pb.NewChunkServerClient(b1Conn)
	b2CS := pb.NewChunkServerClient(b2Conn)

	readVersionReq := &pb.ReadVersionReq{ChunkHandle: chunkHandle}
	readVerResp, err := primaryCS.ReadVersion(ctx, readVersionReq)
	CheckError(t, readVerResp, err, OK)
	readVerResp1, err := b1CS.ReadVersion(ctx, readVersionReq)
	CheckError(t, readVerResp1, err, OK)
	readVerResp2, err := b2CS.ReadVersion(ctx, readVersionReq)
	CheckError(t, readVerResp2, err, OK)

	pVersion := readVerResp.GetVersion()
	b1Version := readVerResp1.GetVersion()
	b2Version := readVerResp2.GetVersion()
	if pVersion != b1Version || b1Version != b2Version {
		t.Errorf("Read Version from 3 Chunk Server do not match")
	}

	readReq := &pb.ReadReq{
		ChunkHandle: chunkHandle,
		Token:       "client#1",
		Start:       0,
		End:         0,
	}

	readResp, err := primaryCS.Read(ctx, readReq)
	CheckError(t, readResp, err, OK)
	readResp1, err := b1CS.Read(ctx, readReq)
	CheckError(t, readResp1, err, OK)
	readResp2, err := b2CS.Read(ctx, readReq)
	CheckError(t, readResp2, err, OK)

	readData := readResp.GetFileData()
	readData1 := readResp1.GetFileData()
	readData2 := readResp2.GetFileData()

	if !bytes.Equal(readData, readData1) || !bytes.Equal(readData1, readData2) || !bytes.Equal(readData2, appendData) {
		t.Errorf("Read Data from 3 Chunk Server do not match")
	}

	start := len(appendData) / 2

	readOffsetReq := &pb.ReadReq{
		ChunkHandle: chunkHandle,
		Token:       "client#1",
		Start:       uint32(start),
		End:         0,
	}

	readOffsetResp, err := primaryCS.Read(ctx, readOffsetReq)
	CheckError(t, readOffsetResp, err, OK)
	readOffsetResp1, err := b1CS.Read(ctx, readOffsetReq)
	CheckError(t, readOffsetResp1, err, OK)
	readOffsetResp2, err := b2CS.Read(ctx, readOffsetReq)
	CheckError(t, readOffsetResp2, err, OK)

	readOffsetData := readOffsetResp.GetFileData()
	readOffsetData1 := readOffsetResp1.GetFileData()
	readOffsetData2 := readOffsetResp2.GetFileData()

	if !bytes.Equal(readOffsetData, readOffsetData1) || !bytes.Equal(readOffsetData1, readOffsetData2) || !bytes.Equal(readOffsetData2, appendData[start:]) {
		t.Errorf("Read Offset Data from 3 Chunk Server do not match")
	}
}

func TestChangeToPrimary(t *testing.T) {
	t.Log("Running TestChangeToPrimary...")
	port := 12345
	path := t.TempDir()
	backupServer := buildChunkServer(t, uint32(port), "", 0, path, nil)
	chunkHandle := "chunk#1"
	metaData := ChunkMetaData{
		ChunkLocation:      path,
		Role:               Secondary,
		PrimaryChunkServer: "localHost:12346",
		PeerAddress:        nil,
		Used:               0,
		Version:            0,
		MetaDataLock:       sync.Mutex{},
		GetVersionChannel:  nil,
	}
	backupServer.Chunks[chunkHandle] = &metaData
	peers := []string{"localHost:12347", "localHost:12348"}
	req := &pb.ChangeToPrimaryReq{
		ChunkHandle: chunkHandle,
		Role:        Primary,
		Peers:       peers,
	}
	res, err := backupServer.ChangeToPrimary(context.Background(), req)
	if err != nil {
		t.Errorf("Get Error response: %s", err)
	}
	if res.GetStatus().GetStatusCode() != OK {
		t.Errorf("Get Error Status Message: %s", res.GetStatus().GetErrorMessage())
	}
	newMeta, ok := backupServer.Chunks[chunkHandle]
	if !ok {
		t.Errorf("Chunk Handle not exists")
	}
	newRole := newMeta.Role
	if newRole != Primary {
		t.Errorf("Role in metaData is not updated")
	}
	newPrimary := newMeta.PrimaryChunkServer
	if newPrimary != "" {
		t.Errorf("PrimaryChunkServer in metaData is not updated")
	}
	newPeers := newMeta.PeerAddress
	if !reflect.DeepEqual(peers, newPeers) {
		t.Errorf("Peers in metaData is not updated")
	}

	// Test Chunkhandle not exists
	chunkHandleNotExist := "Chunk#2"
	req = &pb.ChangeToPrimaryReq{
		ChunkHandle: chunkHandleNotExist,
		Role:        Primary,
		Peers:       peers,
	}
	res, err = backupServer.ChangeToPrimary(context.Background(), req)
	if err != nil {
		t.Errorf("Get Error response: %s", err)
	}
	if res.GetStatus().GetStatusCode() != ERROR_CHUNK_NOT_EXISTS {
		t.Errorf("Get Error Status Message: %s", res.GetStatus().GetErrorMessage())
	}

	// Test is Primary
	chunkHandleIsPrimary := "chunk#3"
	metaDataIsPrimary := ChunkMetaData{
		ChunkLocation:      path,
		Role:               Primary,
		PrimaryChunkServer: "",
		PeerAddress:        peers,
		Used:               0,
		Version:            0,
		MetaDataLock:       sync.Mutex{},
		GetVersionChannel:  nil,
	}
	backupServer.Chunks[chunkHandleIsPrimary] = &metaDataIsPrimary
	// peers := []string{"localHost:12347", "localHost:12348"}
	req = &pb.ChangeToPrimaryReq{
		ChunkHandle: chunkHandleIsPrimary,
		Role:        Primary,
		Peers:       peers,
	}
	res, err = backupServer.ChangeToPrimary(context.Background(), req)
	if err != nil {
		t.Errorf("Get Error response: %s", err)
	}
	if res.GetStatus().GetStatusCode() != ERROR_NOT_SECONDARY {
		t.Errorf("Get Error Status Message: %s", res.GetStatus().GetErrorMessage())
	}
}

func TestAssignNewPrimary(t *testing.T) {
	t.Log("Running TestAssignNewPrimary...")
	port := 12345
	backupPath := t.TempDir()
	backupServer := buildChunkServer(t, uint32(port), "", 0, backupPath, nil)
	newPrimary := "localHost:12346"

	// TODO: Now is Primary, return error
	primaryChunkHandle := "Chunk#1"
	primaryMeta := &ChunkMetaData{
		ChunkLocation:      backupPath,
		Role:               Primary,
		PrimaryChunkServer: "",
		PeerAddress:        nil,
		Used:               0,
		Version:            0,
		MetaDataLock:       sync.Mutex{},
		GetVersionChannel:  nil,
	}
	backupServer.Chunks[primaryChunkHandle] = primaryMeta
	reqIsPrimary := &pb.AssignNewPrimaryReq{
		ChunkHandle: primaryChunkHandle,
		Primary:     newPrimary,
	}
	res, err := backupServer.AssignNewPrimary(context.Background(), reqIsPrimary)
	if err != nil {
		t.Errorf("Get Error response: %s", err)
	}
	if res.GetStatus().GetStatusCode() != ERROR_NOT_SECONDARY {
		t.Errorf("Get Error Status Message: %s", res.GetStatus().GetErrorMessage())
	}

	// TODO: Now is Backup, and Chunk already exist, update primary return OK
	backupChunkHandle := "Chunk#2"
	backupMeta := &ChunkMetaData{
		ChunkLocation:      backupPath,
		Role:               Secondary,
		PrimaryChunkServer: "localHost:12347",
		PeerAddress:        nil,
		Used:               0,
		Version:            0,
		MetaDataLock:       sync.Mutex{},
		GetVersionChannel:  nil,
	}
	backupServer.Chunks[backupChunkHandle] = backupMeta
	backupReq := &pb.AssignNewPrimaryReq{
		ChunkHandle: backupChunkHandle,
		Primary:     newPrimary,
	}
	res, err = backupServer.AssignNewPrimary(context.Background(), backupReq)
	if err != nil {
		t.Errorf("Get Error response: %s", err)
	}
	if res.GetStatus().GetStatusCode() != OK {
		t.Errorf("Get Error Status Message: %s", res.GetStatus().GetErrorMessage())
	}
	newMeta, ok := backupServer.Chunks[backupChunkHandle]
	if !ok {
		t.Errorf("Chunk Handle not exists")
	}
	if newMeta.PrimaryChunkServer != newPrimary {
		t.Errorf("Primary Address in meta is not updated")
	}
	// TODO: Chunk not exists, create new chunkFIle return OK
	primaryPort := 12346
	primaryPath := t.TempDir()
	debugChan := make(chan DebugInfo)
	defer close(debugChan)
	primaryServer := buildChunkServer(t, uint32(primaryPort), "", 8080, primaryPath, debugChan)
	notExistChunkHandle := "Chunk#3"
	primaryMetaData := &ChunkMetaData{
		ChunkLocation:      "",
		Role:               Primary,
		PrimaryChunkServer: "",
		PeerAddress:        nil,
		Used:               0,
		Version:            0,
		MetaDataLock:       sync.Mutex{},
		GetVersionChannel:  nil,
	}
	primaryServer.Chunks[notExistChunkHandle] = primaryMetaData
	go startChunkServer(t, primaryServer)
	notExistReq := &pb.AssignNewPrimaryReq{
		ChunkHandle: notExistChunkHandle,
		Primary:     newPrimary,
	}
	res, err = backupServer.AssignNewPrimary(context.Background(), notExistReq)
	if err != nil {
		t.Errorf("Get Error response: %s", err)
	}
	if res.GetStatus().GetStatusCode() != OK {
		t.Errorf("Get Error Status Message: %s", res.GetStatus().GetErrorMessage())
	}
	newMeta, ok = backupServer.Chunks[notExistChunkHandle]
	if !ok {
		t.Errorf("Chunk Handle not exists")
	}
	// path
	if newMeta.ChunkLocation != backupPath+"/"+notExistChunkHandle {
		t.Errorf("Chunk Location is not updated")
	}
	// role
	if newMeta.Role != Secondary {
		t.Errorf("Chunk Role is not correct")
	}
	// primary address
	if newMeta.PrimaryChunkServer != newPrimary {
		t.Errorf("Primary Address is not updated")
	}
	// timer?
	time.Sleep(time.Duration(100) * time.Millisecond)
	counter := 0
	for counter < 1 {
		select {
		case info := <-debugChan:
			if strings.Compare("GetVersion", info.Func) == 0 && strings.Compare(info.Addr, primaryServer.ServerName) == 0 {
				counter += 1
				t.Logf("Got debug info: Addr: %s, Func: %s, StatusCode: %d", info.Addr, info.Func, info.StatusCode)
			} else {
				t.Errorf("Got not expected debug info: Addr: %s, Func: %s, StatusCode: %d", info.Addr, info.Func, info.StatusCode)
			}
		}
	}
}

func TestUpdateBackup(t *testing.T) {
	t.Log("Running TestUpdateBackup...")
	ctx := context.Background()
	p, b1, b2, debugChan := BuildAndRunThreeChunkServers(t, &BuildChunkServerConfig{
		PrimaryPort: 12345,
		Backup1Port: 12346,
		Backup2Port: 12347,
		MasterHost:  "",
		MasterPort:  0,
	})

	defer close(debugChan)
	time.Sleep(time.Duration(100) * time.Millisecond)

	chunkHandle := "chunk#1"

	primaryConn, err := NewPeerConn(p.ServerName)
	if err != nil {
		t.Errorf("failed to connect to primary goroutine: %v", err)
	}
	defer primaryConn.Close()
	primaryCS := pb.NewChunkServerClient(primaryConn)
	peers := []string{b1.ServerName, b2.ServerName}
	req := &pb.UpdateBackupReq{
		ChunkHandle: chunkHandle,
		Peers:       peers,
	}
	createReq := &pb.CreateChunkReq{
		ChunkHandle: chunkHandle,
		Role:        Primary,
		Primary:     "",
		Peers:       nil,
	}
	createRes, err := primaryCS.CreateChunk(ctx, createReq)
	CheckError(t, createRes, err, OK)

	res, err := primaryCS.UpdateBackup(ctx, req)
	if err != nil {
		t.Errorf("failed to connect to primary server: %v", err)
	}
	if res.GetStatus().GetStatusCode() != OK {
		t.Errorf("expect OK, got %s", res.GetStatus().GetErrorMessage())
	}

	pMeta, ok := p.Chunks[chunkHandle]
	if !ok {
		t.Errorf("chunk metadata not in primary server")
	}

	if !reflect.DeepEqual(pMeta.PeerAddress, peers) {
		t.Errorf("peers do not match")
	}

	// let backups know of the new primary
	b1Conn, err := NewPeerConn(b1.ServerName)
	if err != nil {
		t.Errorf("failed to connect to backup1 goroutine: %v", err)
	}
	defer b1Conn.Close()
	b2Conn, err := NewPeerConn(b2.ServerName)
	if err != nil {
		t.Errorf("failed to connect to backup2 goroutine: %v", err)
	}
	defer b2Conn.Close()

	b1CS := pb.NewChunkServerClient(b1Conn)
	b2CS := pb.NewChunkServerClient(b2Conn)
	newPrimaryReq := &pb.AssignNewPrimaryReq{
		ChunkHandle: chunkHandle,
		Primary:     p.ServerName,
	}

	newPrimaryRes, err := b1CS.AssignNewPrimary(context.Background(), newPrimaryReq)
	CheckError(t, newPrimaryRes, err, OK)
	newPrimaryRes, err = b2CS.AssignNewPrimary(context.Background(), newPrimaryReq)
	CheckError(t, newPrimaryRes, err, OK)

	payload := []byte("testAppendAfterReassign")
	appendReq := &pb.AppendDataReq{
		ChunkHandle: chunkHandle,
		FileData:    payload,
		Token:       "token#1",
		Uuid:        "uuid#1",
	}

	appendRes, err := p.AppendData(ctx, appendReq)
	CheckError(t, appendRes, err, OK)
	primaryExists, backupExists1, backupExists2 := checkFileExists(p.BasePath+"/"+chunkHandle), checkFileExists(b1.BasePath+"/"+chunkHandle), checkFileExists(b2.BasePath+"/"+chunkHandle)
	if !primaryExists {
		t.Errorf("primary chunk file does not exist")
	} else if !backupExists1 {
		t.Errorf("backup1 chunk file does not exist")
	} else if !backupExists2 {
		t.Errorf("backup2 chunk file does not exist")
	}

	// case: chunk not exist on primary
	nonExistChunkReq := &pb.UpdateBackupReq{
		ChunkHandle: "Chunk#2",
		Peers:       peers,
	}

	res, err = p.UpdateBackup(ctx, nonExistChunkReq)
	CheckError(t, res, err, ERROR_CHUNK_NOT_EXISTS)

	// case: chunk is not primary
	invalidPeers := []string{p.ServerName, b2.ServerName}
	res, err = b1.UpdateBackup(ctx, &pb.UpdateBackupReq{
		ChunkHandle: chunkHandle,
		Peers:       invalidPeers,
	})

	CheckError(t, res, err, ERROR_NOT_PRIMARY)
}
