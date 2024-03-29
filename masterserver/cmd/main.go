package main

import (
	"context"
	"encoding/json"
	"github.com/go-redis/redis/v8"
	"log"
	"net"

	ms "github.com/tyromancer/cdfs/masterserver"
	pb "github.com/tyromancer/cdfs/pb"
	"google.golang.org/grpc"
)

func recoverFromDB(client *redis.Client, key string) map[string][]*ms.HandleMetaData {
	result, err := client.HGetAll(context.Background(), key).Result()
	if err != nil {
		if err == redis.Nil {
			log.Println("no records in DB")
		} else {
			log.Printf("failed to connect to DB")
			panic(err)
		}
	}
	ans := make(map[string][]*ms.HandleMetaData)
	for key, val := range result {
		var structSlice []*ms.HandleMetaData
		err := json.Unmarshal([]byte(val), &structSlice)
		if err != nil {
			log.Printf("failed to unmarshall data for key %s", key)
			panic(err)
		}
		ans[key] = structSlice
	}

	return ans
}

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
		log.Printf("failed to connect to redis: %v", err)
		panic(err)
	}

	// read and recover data from db
	files := recoverFromDB(client, "files")

	s := ms.MasterServer{
		Files:           files,
		HandleToMeta:    make(map[string]*ms.HandleMetaData),
		ChunkServerLoad: make(map[string]uint),
		ServerName:      "SuperMaster",
		BasePath:        "",
		CSToHandle:      make(map[string][]*ms.HandleMetaData),
		HeartBeatMap:    make(map[string]*ms.ChunkServerChan),
		DB:              client,
	}

	// TODO: check if there is an available Database
	// TODO: has database: regenerate HandleToMeta from Files map
	err = s.GenerateHandleToMetaMap()
	if err != nil {
		log.Println("Fail to reboot the master, received err: ", err)
		panic(err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterMasterServer(grpcServer, &s)

	if err = grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve master server gRPC on port 8080: %v", err)
	}
}
