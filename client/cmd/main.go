package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	pb "github.com/tyromancer/cdfs/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	ChunkSize = uint32(67108864)
)

const (
	defaultMaster      = ""
	defaultChunkServer = ""
	defaultOps         = ""
	defaultFileName    = ""
	defaultSource      = "client/cmd/data.txt"
	defaultTarget      = ""
)

var (
	ms       = flag.String("ms", defaultMaster, "the address to connect to master")
	cs       = flag.String("cs", defaultChunkServer, "the address to connect to chunk server")
	ops      = flag.String("ops", defaultOps, "the operation: create, append, delete, read")
	filename = flag.String("filename", defaultFileName, "file to operation")
	source   = flag.String("source", defaultSource, "source data file")
	filesize = flag.Uint64("size", 0, "file size")
	offset   = flag.Uint64("offset", 0, "offset")
	target   = flag.String("target", defaultTarget, "target file to store data")
)

var seqNum = 0 

func argsCheck() bool {
	if *ops == "" {
		fmt.Println("You should enter operation")
		return false
	}

	return true
}

func createFile(filename string) {
	conn, err := grpc.Dial(*ms, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	masterConn := pb.NewMasterClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	res, err := masterConn.Create(ctx, &pb.CreateReq{})
	if err != nil || res.GetStatus().GetStatusCode() != 0 {
		log.Fatalf("create file error:  %v %v", res, err)
	}
}

func deleteFile(filename string) {
	conn, err := grpc.Dial(*ms, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	masterConn := pb.NewMasterClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	res, err := masterConn.Delete(ctx, &pb.DeleteReq{})
	if err != nil || res.GetStatus().GetStatusCode() != 0 {
		log.Fatalf("delete file error:  %v %v", res, err)
	}
}

func appendFile(filename string, sourceFile string, fileSize uint64) {
	conn, err := grpc.Dial(*ms, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	masterConn := pb.NewMasterClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	res, err := masterConn.AppendFile(ctx, &pb.AppendFileReq{})
	if err != nil || res.GetStatus().GetStatusCode() != 0 {
		log.Fatalf("append file error:  %v %v", res, err)
	}
	primaryIp := res.GetPrimaryIP()
	chunkHandle := res.GetChunkHandle()
	data := []byte("hello world") // TODO
	for i := 0; i < len(chunkHandle); i++ {
		if !appendToChunk(seqNum, primaryIp[i], chunkHandle[i], data) {
			log.Fatalf("append to chunk error:  %v %v", res, err)
		}else{
			seqNum += 1
		}
	}
	fmt.Println("success")
}

func appendToChunk(seqNum int, primaryIp string, chunkHandle string, data []byte) bool {
	// Create chunk server client to talk to chunk server
	var csConn *grpc.ClientConn
	csConn, err := grpc.Dial(primaryIp, grpc.WithInsecure())

	if err != nil {
		log.Fatalf("Failed to connect to chunk server")
		return false
	}

	appendDataReq := &pb.AppendDataReq{
		SeqNum:      uint32(seqNum),
		ChunkHandle: chunkHandle,
		FileData:    data,
		Token:       "Client#1", //TODO
	}
	csClient := pb.NewChunkServerClient(csConn)
	appendDataRes, err := csClient.AppendData(context.Background(), appendDataReq)
	if err != nil || appendDataRes.GetStatus().GetStatusCode() != 0 {
		log.Fatalf("Error when calling append data: %v", err)
		return false
	}

	return true
}

// TODO 
func readFile(filename string, offset uint64) ([]byte, error) {
	conn, err := grpc.Dial(*ms, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
		return nil, err
	}
	defer conn.Close()
	masterConn := pb.NewMasterClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	res, err := masterConn.GetLocation(ctx, &pb.GetLocationReq{})
	if err != nil || res.GetStatus().GetStatusCode() != 0 {
		log.Fatalf("append file error:  %v %v", res, err)
		if res.GetStatus().GetStatusCode() != 0{
			return nil, fmt.Errorf(res.GetStatus().GetErrorMessage())
		}
		return nil, err 
	}
	// TODO
	// primaryIp := res.GetPrimaryIP()
	// chunkHandle := res.GetChunkHandle()
	// data := []byte("hello world") // TODO
	// for i := 0; i < len(chunkHandle); i++ {
	// 	if !appendToChunk(i, primaryIp[i], chunkHandle[i], data) {
	// 		log.Fatalf("append to chunk error:  %v %v", res, err)
	// 	}
	// }
	// fmt.Println("success")
	
	
	return nil, nil
}

func main() {
	flag.Parse()
	if !argsCheck() {
		return
	}
	switch *ops {
	case "create":
		createFile(*filename)
	case "delete":
		deleteFile(*filename)
	case "append":
		appendFile(*filename, *source, *filesize)
	case "read":
		res, err := readFile(*filename, *offset)
		if err != nil {
			fmt.Println(err)
			return
		}
		if *target == "" {
			fmt.Println(string(res))
		} else {
			//TODO todostore into target file
		}
	}
}
