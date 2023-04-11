package main

import (
	"fmt"
	"github.com/go-redis/redis/v8"
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
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	_, err = client.Ping(client.Context()).Result()
	if err != nil {
		fmt.Printf("failed to connect to redis: %v", err)
		return
	}

	s := ms.MasterServer{Files: make(map[string][]*ms.HandleMetaData), HandleToMeta: make(map[string]*ms.HandleMetaData), ChunkServerLoad: make(map[string]uint), ServerName: "SuperMaster", BasePath: "", CSToHandle: make(map[string][]*ms.HandleMetaData), HeartBeatMap: make(map[string]*ms.ChunkServerChan)}

	grpcServer := grpc.NewServer()
	pb.RegisterMasterServer(grpcServer, &s)

	if err = grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve master server gRPC on port 8080: %v", err)
	}
}
