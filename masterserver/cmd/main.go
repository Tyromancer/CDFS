package main

import (
	"log"
	"net"

	ms "github.com/tyromancer/cdfs/masterserver"
	pb "github.com/tyromancer/cdfs/pb"
	"google.golang.org/grpc"
)

func main() {
	lis, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("Failed to listen on port 8080 %v", err)
	}

	s := ms.MasterServer{}
	grpcServer := grpc.NewServer()
	pb.RegisterMasterServer(grpcServer, &s)

	if err = grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve master server gRPC on port 8080: %v", err)
	}
}
