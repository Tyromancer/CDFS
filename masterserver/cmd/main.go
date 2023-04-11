package main

import (
	"fmt"
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

	// TODO: check if there is an available Database
	// TODO: has database: regenerate HandleToMeta from Files map
	//err = s.GenerateHandleToMetaMap()
	//if err != nil {
	//	log.Println("Fail to reboot the master, received err: ", err)
	//	return
	//}

	grpcServer := grpc.NewServer()
	pb.RegisterMasterServer(grpcServer, &s)

	if err = grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve master server gRPC on port 8080: %v", err)
	}
}
