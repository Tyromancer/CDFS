package masterserver

import (
	"testing"
	"reflect"
)


func TestTreeMapChunkServerLoad(t *testing.T) {
	t.Log("Running TestTreeMapChunkServerLoad...")
	chunkServerLoad := make(map[string]uint)
	chunkServerLoad["ChunkServer1"] = uint(128)
	chunkServerLoad["ChunkServer2"] = uint(64)
	chunkServerLoad["ChunkServer3"] = uint(256)
	chunkServerLoad["ChunkServer4"] = uint(1028)
	lowestThree := lowestThreeChunkServer(chunkServerLoad)
	expected := []string{"ChunkServer2", "ChunkServer1", "ChunkServer3"}
	
	if !reflect.DeepEqual(lowestThree, expected) {
		t.Errorf("The TreeMap does not sort on values, expected %v, but get %v", expected, lowestThree)
	}

}