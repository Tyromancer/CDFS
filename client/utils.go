package client

import (
	"bufio"
	"io"
	"os"

	"github.com/google/uuid"
)

func genUuid() string {
	uuid := uuid.New()
	key := uuid.String()
	return key
}

func ReadUserFile(sourceFile string) ([][]byte, int64, error) {
	fi, err := os.Stat(sourceFile)
	if err != nil {
		return nil, 0, err
	}
	size := fi.Size()
	// data := make([][]byte, (size+int64(ChunkSize)-1)/int64(ChunkSize))
	var data [][]byte
	f, err := os.Open(sourceFile)
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()
	buf := make([]byte, ChunkSize)
	for {
		l, err := f.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, 0, err
		}
		data = append(data, buf[:l])
	}
	return data, size, nil
}

func WriteUserFile(targetPath string, data []byte) error {
	f, err := os.OpenFile(targetPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	defer f.Close()
	writer := bufio.NewWriter(f)
	if _, err = writer.Write(data); err != nil {
		return err
	}
	if err = writer.Flush(); err != nil {
		return err
	}
	return nil
}
