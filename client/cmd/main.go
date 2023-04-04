package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/avast/retry-go"
	. "github.com/tyromancer/cdfs/client"
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
	target   = flag.String("target", defaultTarget, "target path to store data")
)

func argsCheck() bool {
	if *ops == "" {
		fmt.Println("You should enter operation")
		return false
	}

	return true
}

// Main function
func main() {
	flag.Parse()
	if !argsCheck() {
		return
	}
	switch *ops {
	case "create":
		CreateFile(*ms, *filename)
	case "delete":
		DeleteFile(*ms, *filename)
	case "append":
		data, size, err := ReadUserFile(*source)
		if err != nil {
			fmt.Printf("Read file error: %+v", err)
			return
		}
		err = retry.Do(
			func() error {
				return AppendFile(*ms, *filename, data, uint64(size))
			},
			retry.DelayType(retry.FixedDelay),
			retry.Delay(time.Second),
			retry.Attempts(3),
		)
		if err != nil {
			fmt.Println("Fail")
		} else {
			fmt.Println("success")
		}
	case "read":
		data, err := ReadFile(*ms, *filename, uint32(*offset), uint32(*filesize))
		if err != nil {
			fmt.Println(err)
			return
		}
		if *target == "" {
			fmt.Println(string(data))
		} else {
			if err := WriteUserFile(*target, data); err != nil {
				fmt.Printf("Write data to path error %+v", err)
			}
		}
	}
}
