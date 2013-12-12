package merklemap

import (
	"testing"
	"code.google.com/p/goprotobuf/proto"
)

func TestMarshalUnmarshalLookupResult(t *testing.T) {
	tree, err := Open("tree.dat")
	snapshot := tree.GetSnapshot(0)
	handle, err := snapshot.OpenHandle()
	if err != nil {
		panic(err)
	}
	key := [32]byte{1,2,3,4,5,6,7,8,9,10,11,12,13,14,15}
	val := [32]byte{31,30,29,28,27,26,25,24,23,22,21,20,19,18,17,16}
	handle.Set(&key, &val)
	key[31]++; val[31]++;
	handle.Set(&key, &val)
	key[30]++; val[30]++;
	handle.Set(&key, &val)
	key[30]--; val[30]--;
	path, err := handle.GetPath(&key)
	if err != nil {
		panic(err)
	}
	bs, err := proto.Marshal(path)
	if err != nil {
		panic(err)
	}
	path2 := new(LookupResult)
	err = proto.Unmarshal(bs, path2)
	if err != nil {
		panic(err)
	}
}

