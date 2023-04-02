package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/avast/retry-go"
	"github.com/google/uuid"
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

// Util
func argsCheck() bool {
	if *ops == "" {
		fmt.Println("You should enter operation")
		return false
	}

	return true
}

func genUuid() string{
	uuid := uuid.New()
	key := uuid.String()
	return key 
}

//TODO: 
func readUserFile(sourceFile string) ([]byte, error){
	return []byte("hello world"), nil 
}


// RPC function

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

func appendFile(filename string, data []byte, fileSize uint64) error {
	conn, err := grpc.Dial(*ms, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("[%v] connect error: %v", "master", err)
		return err 
	}
	defer conn.Close()
	masterConn := pb.NewMasterClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	reg, err := masterConn.GetToken(ctx, &pb.GetTokenReq{})
	if err != nil {
		log.Fatalf("[%v] client register error: %v","master", err)
		return err 
	}
	uuid := genUuid()
	appendReq := pb.AppendFileReq{
		FileName: filename,
		FileSize: uint32(fileSize),
		Uuid: uuid,
	}
	res, err := masterConn.AppendFile(ctx, &appendReq)
	if err != nil || res.GetStatus().GetStatusCode() != 0 {
		log.Fatalf("master append file error:  %v %v", res, err)
		return err 
	}
	primaryIps := res.GetPrimaryIP()
	chunkHandles := res.GetChunkHandle()
	
	chunckCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	successChan := make(chan int, len(chunkHandles))
	defer close(successChan)
	failChan := make(chan error, len(chunkHandles))
	defer close(failChan)
	for i:= 0; i< len(chunkHandles); i++{
		curData := data[i*int(ChunkSize): (i+1) * int(ChunkSize)]
		go appendToChunk(chunckCtx, primaryIps[i], chunkHandles[i], curData, reg.GetUniqueToken(), successChan, failChan)
	}	
	count := 0
	for{
		select{
		case  err := <- failChan:
			fmt.Printf("append to chunk fail")
			cancel()
			return err 
		case <- successChan:
			count += 1
			if(count == len(chunkHandles)){
				return nil 
			}
		case <- time.After(5 * time.Second):
			fmt.Printf("append to chunk fail")
			cancel()
			return fmt.Errorf("chunk server timeout")
		}
	}
}

func appendToChunk(ctx context.Context, primaryIp string, chunkHandle string, data []byte, token string, successChan chan int, failChan chan error) {
	// Create chunk server client to talk to chunk server
	var csConn *grpc.ClientConn
	csConn, err := grpc.Dial(primaryIp, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect to chunk server")
		failChan <- err 
		return 
	}

	appendDataReq := &pb.AppendDataReq{
		ChunkHandle: chunkHandle,
		FileData:    data,
		Token:       token, 
		Uuid: genUuid(),
	}
	csClient := pb.NewChunkServerClient(csConn)
	appendDataRes, err := csClient.AppendData(ctx, appendDataReq)
	if err != nil || appendDataRes.GetStatus().GetStatusCode() != 0 {
		log.Fatalf("Error when calling append data: %v", err)
		failChan <- err
	}
	successChan <- 0
}

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

// Main function 
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
		data, err := readUserFile(*source)
		if err != nil{
			fmt.Printf("Read file error: %v", err)
			return 
		}
		err = retry.Do(
			func () error{
				return appendFile(*filename, data, *filesize)
			},
			retry.DelayType(retry.FixedDelay), 
			retry.Delay(time.Second), 
			retry.Attempts(3),
		)
		if err != nil{
			fmt.Println("Fail")
		}else{
			fmt.Println("success")
		}
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
