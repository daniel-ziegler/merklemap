package main

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"testing"
)

func TestRandomly(t *testing.T) {
	bestTime := 100000
	bestSeed := -1
	var testCount int
	if testing.Short() {
		testCount = 0x10
	} else {
		testCount = 0x100
	}
	for i := 0x0; i < testCount; i++ {
		rand.Seed(int64(0x1234567 + (i ^ (i << 3)) + i*100000007))
		tree, err := Open("tree.dat")
		if err != nil {
			panic(err)
		}
		randSign := rand.Intn(2)*2 - 1
		big := rand.Intn(20) == 0
		var itCount int
		if big {
			itCount = 10000
		} else {
			itCount = 100
		}
		time := mapTest(tree, itCount, (1+rand.Intn(256))*randSign)
		fmt.Printf("seed %x: %v\n", i, time)
		if time < bestTime {
			bestTime = time
			bestSeed = i
		}
	}
	fmt.Printf("SEED %x: %v\n", bestSeed, bestTime)
}

func BenchmarkBigTree(b *testing.B) {
	rand.Seed(0x77)
	tree, err := Open("tree.dat")
	if err != nil {
		panic(err)
	}
	mapTest(tree, 100000, 256)
}

func mapTest(tree *Map, itCount int, byteRange int) int {
	bytez := func(b byte) [32]byte {
		var bytes [32]byte
		for i := range bytes {
			bytes[i] = b<<4 | b
		}
		return bytes
	}
	randBytes := func() [32]byte {
		var bs [32]byte
		if byteRange < 0 {
			bs = bytez(byte(rand.Intn(-byteRange)))
			bs[0] = byte(rand.Intn(-byteRange))
			return bs
		} else {
			for i := range bs {
				bs[i] = byte(rand.Intn(byteRange))
			}
			if dbg > 2 {
				fmt.Printf("rand bytes = %x\n", bs)
			}
			return bs
		}
	}
	snapshot := tree.GetSnapshot(0)
	refMap := map[[32]byte][32]byte{}
	refMapKeys := [][32]byte{}
	randMapKey := func() [32]byte {
		return refMapKeys[rand.Intn(len(refMapKeys))]
	}
	refSet := func(key [32]byte, val [32]byte) {
		if _, present := refMap[key]; !present {
			refMapKeys = append(refMapKeys, key)
		}
		refMap[key] = val
	}
	refGet := func(key [32]byte) [32]byte {
		return refMap[key]
	}
	treeSet := func(key [32]byte, val [32]byte) {
		if dbg > 1 {
			fmt.Fprintf(os.Stdout, "set: [%x] = %x...\n", key, val)
		}
		err := snapshot.Set(&key, &val)
		if err != nil {
			panic(err)
		}
		if dbg > 0 {
			fmt.Fprintf(os.Stdout, "set  [%x] = %x done\n", key, val)
		}
	}
	treeGet := func(key [32]byte) [32]byte {
		if dbg > 2 {
			fmt.Fprintf(os.Stdout, "read [%x]...\n", key)
		}
		result, err := snapshot.GetPath(&key)
		if err != nil {
			panic(err)
		}
		var val [32]byte
		if result == nil {
			val = [32]byte{}
		} else {
			rootHash, err := snapshot.GetRootHash()
			if err != nil {
				panic(err)
			}
			if dbg > 1 {
				fmt.Fprintf(os.Stdout, "Lookup: %x\n", result)
			}
			computedRootHash := result.ComputeRootHash()
			if !bytes.Equal(computedRootHash, rootHash[:]) {
				panic(fmt.Sprintf("bad root hash: %x != %x", computedRootHash, rootHash))
			}
			val = result.Value
		}
		if dbg > 1 {
			fmt.Fprintf(os.Stdout, "read [%x] = %x\n", key, val)
		}
		return val
	}
	for i := 0; i < itCount; i++ {
		if i%1000 == 0 {
			fmt.Printf("operation %v\n", i)
		}
		switch rand.Intn(3) {
		case 0:
			k := randBytes()
			v := randBytes()
			refSet(k, v)
			treeSet(k, v)
		case 1:
			k := randBytes()
			v1 := refGet(k)
			v2 := treeGet(k)
			if dbg > 0 {
				fmt.Printf("1: [%x] = %x, %x\n", k, v2, v1)
			}
			if v1 != v2 {
				panic("wrong 1")
			}
		case 2:
			if len(refMap) > 0 {
				k := randMapKey()
				v1 := refGet(k)
				if dbg > 1 {
					fmt.Printf("read [%x]\n", k)
				}
				v2 := treeGet(k)
				if dbg > 0 {
					fmt.Printf("2: [%x] = %x, %x\n", k, v2, v1)
				}
				if v1 != v2 {
					panic(fmt.Sprintf("wrong 2 (t%v)", i))
					//return i
				}
			}
		}
	}
	return 100000000
}
