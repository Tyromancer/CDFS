package client

import (
	"testing"
)

func TestReadUserFile(t *testing.T) {
	sourceFile := "data/simple.txt"
	data, size, err := ReadUserFile(sourceFile)
	if err != nil {
		t.Error(err)
		return
	}
	t.Logf("size: %v data: \n%v", size, data)
	if data != nil && string(data[0]) != "hello world!" {
		t.Errorf("expected:%v, got:%+v", "hello world", data)
	}
	sourceFile = "data/10M.txt"
	_, size, err = ReadUserFile(sourceFile)
	if err != nil{
		t.Error(err)
		return 
	}
	if size != 10*1024*1024{
		t.Errorf("expected size: %v, actual: %v", 10*1024*1024, size)
		return 
	}
	data,size, err = ReadUserFile("data/80M.txt")
	if err != nil{
		t.Error(err)
		return 
	} 
	if len(data) != 2{
		t.Errorf("error data size: %v, chunk num: %v", size, len(data))
	}
}

