package main

import (
	"log"
	"net"

	cs "github.com/tyromancer/cdfs/chunkserver"
	pb "github.com/tyromancer/cdfs/pb"
	"google.golang.org/grpc"
)

func main() {
	lis, err := net.Listen("tcp", ":12345")
	if err != nil {
		log.Fatalf("Failed to listen on port 12345 %v", err)
	}

	s := cs.ChunkServer{
		BasePath:   "/CDFS",
		HostName:   "localhost",
		Port:       12345,
		MasterIP:   "localhost",
		MasterPort: 8080,
	}

	grpcServer := grpc.NewServer()
	pb.RegisterChunkServerServer(grpcServer, &s)

	// Register chunk server with Master
	err = s.SendRegister()
	if err != nil {
		log.Fatalf("Cannot register chunk server with Master: %v", err)
	}

	// Sending Heartbeat
	timer := cs.HeartBeatTimer{Srv: &s, Timeout: 100}
	go timer.Trigger()

	if err = grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve chunk server gRPC on port 12345: %v", err)
	}
}
