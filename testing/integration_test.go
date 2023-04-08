package testing

import (
	"context"
	"fmt"
	"log"
	"net"
	"testing"
	"time"

	cs "github.com/tyromancer/cdfs/chunkserver"
	ms "github.com/tyromancer/cdfs/masterserver"
	"github.com/tyromancer/cdfs/pb"
	"google.golang.org/grpc"
)

const (
	ChunkSize = uint32(67108864)
)

func NewChunkServer(t *testing.T, port uint32, msPort uint32) {
	t.Log("Create New Chunk Server")
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("Failed to listen on port %d %v", port, err)
	}

	s := cs.ChunkServer{Chunks: make(map[string]*cs.ChunkMetaData), ClientLastResp: make(map[string]cs.RespMetaData), ServerName: fmt.Sprintf("localhost:%d", port), BasePath: t.TempDir(), HostName: "localhost", Port: port}

	err = s.SendRegister()
	if err != nil {
		log.Fatalf("Failed to register on MS: %v", err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterChunkServerServer(grpcServer, &s)

	if err = grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve chunk server: %v", err)
	}
}

func NewMasterServer(t *testing.T, port uint32) {
	t.Log("Create New Master Server")
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("Failed to listen on port %d %v", port, err)
	}
	s := ms.MasterServer{Files: make(map[string][]ms.HandleMetaData), ChunkServerLoad: make(map[string]uint), ServerName: "SuperMaster", BasePath: ""}

	grpcServer := grpc.NewServer()
	pb.RegisterMasterServer(grpcServer, &s)

	t.Log("Master, Start Serve")
	if err = grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve master server gRPC on port 8080: %v", err)
	}

}

func TestChunkServerMasterIntegration(t *testing.T) {

	go NewMasterServer(t, 8080)
	time.Sleep(2 * time.Second)
	go NewChunkServer(t, 12345, 8080)
	time.Sleep(2 * time.Second)

	// create client to talk to master
	var conn *grpc.ClientConn
	conn, err := grpc.Dial(":8080", grpc.WithInsecure())

	if err != nil {
		log.Fatalf("Failed to connect to port 8080: %v", err)
	}

	defer conn.Close()

	c := pb.NewMasterClient(conn)
	req := &pb.CreateReq{FileName: "File1"}
	res, err := c.Create(context.Background(), req)

	if err != nil || res.GetStatus().GetStatusCode() != 0 {
		t.Errorf("Error when calling create: %v", err)
	}

	payload1 := []byte("hello world")
	payload2 := []byte("goodbye world")

	// call master append file rpc
	appendFileReq := &pb.AppendFileReq{
		FileName: "File1",
		FileSize: 2 * ChunkSize,
	}

	appendFileRes, err := c.AppendFile(context.Background(), appendFileReq)
	if err != nil || appendFileRes.GetStatus().GetStatusCode() != 0 {
		t.Errorf("Error when calling append file: %v", err)
	}

	primaryAddrs := appendFileRes.PrimaryIP
	chunkHandles := appendFileRes.ChunkHandle

	t.Log("Primary addrs is ", primaryAddrs)
	t.Log("Chunk handles is ", chunkHandles)

	if len(primaryAddrs) != 2 || len(primaryAddrs) != len(chunkHandles) {
		t.Errorf("Expected 2 primary IP, got %d primary IP and %d chunk handles", len(primaryAddrs), len(chunkHandles))
	}

	// Create chunk server client to talk to chunk server
	var csConn *grpc.ClientConn
	csConn, err = grpc.Dial(":12345", grpc.WithInsecure())

	if err != nil {
		t.Errorf("Failed to connect to chunk server")
	}
	appendDataReq1 := &pb.AppendDataReq{
		SeqNum:      0,
		ChunkHandle: chunkHandles[0],
		FileData:    payload1,
		Token:       "Client#1",
	}

	appendDataReq2 := &pb.AppendDataReq{
		SeqNum:      1,
		ChunkHandle: chunkHandles[1],
		FileData:    payload2,
		Token:       "Client#1",
	}

	t.Logf("Got chunk handles %s, %s", chunkHandles[0], chunkHandles[1])

	csClient := pb.NewChunkServerClient(csConn)
	appendDataRes, err := csClient.AppendData(context.Background(), appendDataReq1)
	if err != nil || appendDataRes.GetStatus().GetStatusCode() != 0 {
		t.Errorf("Error when calling append data: %v", err)
	}

	appendDataRes, err = csClient.AppendData(context.Background(), appendDataReq2)
	if err != nil || appendDataRes.GetStatus().GetStatusCode() != 0 {
		t.Errorf("Error when calling append data: %v", err)
	}

}
