package testing

import (
	"fmt"
	"log"
	"net"
	"reflect"
	"testing"
	"time"

	cs "github.com/tyromancer/cdfs/chunkserver"
	"github.com/tyromancer/cdfs/client"
	ms "github.com/tyromancer/cdfs/masterserver"
	"github.com/tyromancer/cdfs/pb"
	"google.golang.org/grpc"
)

const (
	ChunkSize = uint32(67108864)
)

func NewChunkServer(t *testing.T, port uint32, msPort uint32) {

	// basePath := flag.String("path", "/CDFS", "base directory to store files")

	// TODO: add command line argument checking

	addr := fmt.Sprintf("%s:%d", "", port)
	log.Println("Starting chunk server at ", addr)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("Failed to listen on port 12345 %v", err)
	}

	s := cs.ChunkServer{
		Chunks:         make(map[string]*cs.ChunkMetaData),
		ClientLastResp: make(map[string]cs.RespMetaData),
		ServerName:     cs.GetAddr("", port),
		BasePath:       t.TempDir(),
		HostName:       "localhost",
		Port:           port,
		MasterIP:       "",
		MasterPort:     msPort,
		Debug:          false,
		DebugChan:      nil,
	}

	grpcServer := grpc.NewServer()
	pb.RegisterChunkServerServer(grpcServer, &s)

	// Register chunk server with Master
	err = s.SendRegister()
	if err != nil {
		log.Fatalf("Cannot register chunk server with Master: %v", err)
	}

	if err = grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve chunk server gRPC on port 12345: %v", err)
	}
}

func NewMasterServer(t *testing.T, port uint32) {
	t.Log("Create New Master Server")
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("Failed to listen on port %d %v", port, err)
	}
	s := ms.MasterServer{Files: make(map[string][]*ms.HandleMetaData), HandleToMeta: make(map[string]*ms.HandleMetaData), ChunkServerLoad: make(map[string]uint), ServerName: "SuperMaster", BasePath: ""}

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
	go NewChunkServer(t, 12346, 8080)
	go NewChunkServer(t, 12347, 8080)
	go NewChunkServer(t, 12348, 8080)
	time.Sleep(2 * time.Second)

	filename := "test1"
	master := "localhost:8080"
	data := []byte("hello world!")
	totalData := [][]byte{data}

	client.CreateFile(master, filename)
	err := client.AppendFile(master, filename, totalData, uint64(len(data)))
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	readResult, err := client.ReadFile(master, filename, 0, uint32(len(data)))
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if !reflect.DeepEqual(readResult, data) {
		t.Errorf("Read error expected: %v result: %v", data, readResult)
	}

	client.DeleteFile(master, filename)

}
