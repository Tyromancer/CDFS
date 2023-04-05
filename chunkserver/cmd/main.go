package main

import (
	"flag"
	"fmt"
	cs "github.com/tyromancer/cdfs/chunkserver"
	pb "github.com/tyromancer/cdfs/pb"
	"google.golang.org/grpc"
	"log"
	"net"
)

func main() {
	host := flag.String("host", "", "host address to listen on")
	port := flag.Uint("port", 12345, "port number to listen on")
	basePath := flag.String("path", "/CDFS", "base directory to store files")
	masterHost := flag.String("mhost", "", "master server host address")
	masterPort := flag.Uint("mport", 8080, "master server port number")
	// hbTimeout := flag.Int("hb", 100, "heartbeat timer timeout value (ms)")
	flag.Parse()

	// TODO: add command line argument checking

	addr := fmt.Sprintf("%s:%d", *host, *port)
	log.Println("Starting chunk server at ", addr)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("Failed to listen on port 12345 %v", err)
	}

	//log.Println("Starting connect to master host at %s ", addr)
	s := cs.ChunkServer{
		BasePath:   *basePath,
		HostName:   *host,
		Port:       uint32(*port),
		MasterIP:   *masterHost,
		MasterPort: uint32(*masterPort),
	}

	grpcServer := grpc.NewServer()
	pb.RegisterChunkServerServer(grpcServer, &s)

	// Register chunk server with Master
	err = s.SendRegister()
	if err != nil {
		log.Fatalf("Cannot register chunk server with Master: %v", err)
	}

	// Sending Heartbeat
	//timer := cs.HeartBeatTimer{Srv: &s, Timeout: *hbTimeout}
	//go timer.Trigger()

	if err = grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve chunk server gRPC on port 12345: %v", err)
	}
}
