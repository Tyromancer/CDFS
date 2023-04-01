package test

import (
	"context"
	"log"

	"github.com/tyromancer/cdfs/pb"
	"google.golang.org/grpc"
)

func main() {
	var conn *grpc.ClientConn
	conn, err := grpc.Dial(":12345", grpc.WithInsecure())

	if err != nil {
		log.Fatalf("Failed to connect to port 12345: %v", err)
	}

	defer conn.Close()

	c := pb.NewChunkServerClient(conn)
	req := &pb.ReadReq{ChunkHandle: "0", Token: "TestClientToken"}
	res, err := c.Read(context.Background(), req)

	if err != nil {
		log.Fatalf("Error when calling read: %v", err)
	}

	log.Printf("Response from chunk server: seqNum: %d, fileData: %s", res.SeqNum, string(res.FileData[:]))
}
