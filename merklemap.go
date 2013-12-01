package merklemap

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
)

const dbg = 0

func assert(flag bool) {
	if !flag {
		panic("assertion failed")
	}
}

func min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

type MapError struct {
	msg string
}

func (err *MapError) Error() string {
	return err.msg
}

type Map struct {
	allocMutex sync.Mutex
	fileName   string
}

const HEADER_SIZE = 1024

type header struct {
	NrNodes int64
}

func (header *header) Write(w io.Writer) error {
	err := binary.Write(w, binary.LittleEndian, header)
	if err != nil {
		return err
	}
	// pad
	_, err = w.Write(make([]byte, HEADER_SIZE-binary.Size(header)))
	return err
}

const KEY_BYTES = 32
const HASH_BYTES = 32
const DATA_BYTES = 32
const ELEMENT_BITS = 4
const KEY_ELEMENTS = KEY_BYTES * 8 / ELEMENT_BITS
const NODE_CHILDREN = 1 << ELEMENT_BITS

const NODE_SIZE = 1024

func Hash(data []byte) *[HASH_BYTES]byte {
	hash := sha256.New()
	hash.Write(data)
	hashVal := new([HASH_BYTES]byte)
	copy(hashVal[:], hash.Sum(make([]byte, 0)))
	if dbg > 0 {
		fmt.Fprintf(os.Stdout, "Hash(%x) = %x\n", data, hashVal)
	}
	return hashVal
}

type leafData struct {
	Hash  [HASH_BYTES]byte
	Value [HASH_BYTES]byte
}

type nodeData struct {
	Children    [NODE_CHILDREN]int64
	ChildHashes [NODE_CHILDREN][HASH_BYTES]byte
	leafData
}

type diskNode struct {
	nodeData
	SubstringLength int64 // number of elements (i.e. in units of ELEMENT_BITS bits)
	KeySubstring    [KEY_BYTES]byte
}

type hashNode struct {
	Hash           []byte
	HasTwoChildren bool
}

type node struct {
	nodeData
	Index        int64
	KeyOffset    int
	KeySubstring []byte // slice of elements
	MapHashes    *[NODE_CHILDREN - 1]*hashNode
}

// hardcoded for ELEMENT_BITS = 4
func bytesToElements(bytes []byte) []byte {
	if ELEMENT_BITS != 4 {
		panic("not implemented")
	}
	elements := make([]byte, len(bytes)*2)
	for i, b := range bytes {
		elements[i*2] = b >> 4
		elements[i*2+1] = b & 0xf
	}
	return elements
}

// hardcoded for ELEMENT_BITS = 4
func elementsToBytes(elements []byte) []byte {
	if ELEMENT_BITS != 4 {
		panic("not implemented")
	}
	bytes := make([]byte, len(elements)/2)
	for i := range bytes {
		bytes[i] = elements[i*2]<<4 | elements[i*2+1]
	}
	if len(elements)%2 == 1 {
		bytes = append(bytes, elements[len(elements)-1]<<4)
	}
	return bytes
}

func (n *node) BuildMapHashes(treeIx int) []byte {
	leafIx := treeIx - NODE_CHILDREN + 1
	if leafIx >= 0 {
		// leaf
		if n.Children[leafIx] > 0 {
			return n.ChildHashes[leafIx][:]
		} else {
			return make([]byte, 0)
		}
	} else {
		hn := &hashNode{Hash: make([]byte, 0)}
		for i := 0; i < 2; i++ {
			hn.Hash = append(hn.Hash, n.BuildMapHashes(treeIx*2+1+i)...) // might be empty
		}
		if len(hn.Hash) > HASH_BYTES {
			// only hash if both children were non-empty
			hn.HasTwoChildren = true
			hn.Hash = Hash(hn.Hash)[:]
		}
		n.MapHashes[treeIx] = hn
		return hn.Hash
	}
}

func (n *node) EnsureMapHashes() *[NODE_CHILDREN - 1]*hashNode {
	if n.MapHashes == nil {
		n.MapHashes = new([NODE_CHILDREN - 1]*hashNode)
		n.BuildMapHashes(0)
	}
	return n.MapHashes
}

// Indexes into the binary tree (row = depth, column = in-order index in row)
func (n *node) IndexHashNode(row int, column int) int {
	return (1 << uint(row)) - 1 + column
}

func (n *node) getHashNode(index int) *hashNode {
	leafIx := index - NODE_CHILDREN + 1
	if leafIx >= 0 {
		return &hashNode{Hash: n.ChildHashes[leafIx][:]}
	}
	treeHashes := n.EnsureMapHashes()
	return treeHashes[index]
}

func (n *node) setChildHash(childIx int, hash []byte) {
	copy(n.ChildHashes[childIx][:], hash)
	n.MapHashes = nil // invalidate
}

func (parent *node) SetChild(childIx int, child *node) {
	parent.Children[childIx] = child.Index
	// propagate hash
	if child.KeyOffset+len(child.KeySubstring) == KEY_ELEMENTS {
		// leaf
		parent.setChildHash(childIx, child.Hash[:])
	} else {
		parent.setChildHash(childIx, child.getHashNode(child.IndexHashNode(0, 0)).Hash)
	}
}

func (dn *diskNode) fromDisk(index int64, keyOffset int) *node {
	n := new(node)
	n.nodeData = dn.nodeData
	n.Index = index
	n.KeyOffset = keyOffset
	n.KeySubstring = bytesToElements(dn.KeySubstring[:])[:dn.SubstringLength]
	return n
}

func (n *node) toDisk() *diskNode {
	dn := new(diskNode)
	dn.nodeData = n.nodeData
	copy(dn.KeySubstring[:], elementsToBytes(n.KeySubstring))
	dn.SubstringLength = int64(len(n.KeySubstring))
	return dn
}

func (snapshot *Snapshot) clear() error {
	_, err := snapshot.file.Seek(0, os.SEEK_SET)
	if err != nil {
		return err
	}
	var header header
	header.NrNodes = 0
	err = header.Write(snapshot.file)
	if err != nil {
		return err
	}
	offset, err := snapshot.file.Seek(0, os.SEEK_CUR)
	if err != nil {
		return err
	}
	err = snapshot.file.Truncate(offset)
	return err
}

func Open(fileName string) (*Map, error) {
	tree := new(Map)
	tree.fileName = fileName
	snapshot := tree.GetSnapshot(-1)
	err := snapshot.openFile()
	if err != nil {
		return nil, err
	}
	defer snapshot.closeFile()
	header := new(header)
	err = binary.Read(snapshot.file, binary.LittleEndian, header)
	if err != nil {
		// Initialize file
		return tree, snapshot.clear()
	} else {
		return tree, nil
	}
}

func (snapshot *Snapshot) readNode(nodeIx int64, keyOffset int) (*node, error) {
	dn := new(diskNode)
	_, err := snapshot.file.Seek(int64(HEADER_SIZE+NODE_SIZE*(nodeIx-1)), os.SEEK_SET)
	if err != nil {
		return nil, err
	}
	err = binary.Read(snapshot.file, binary.LittleEndian, dn)
	if err != nil {
		return nil, err
	}
	if dbg > 2 {
		fmt.Fprintf(os.Stdout, " read %x: %x\n", nodeIx, dn)
	}
	// debug check
	header, err := snapshot.readHeader()
	if err != nil {
		panic(err)
	}
	n := dn.fromDisk(nodeIx, keyOffset)
	for i, c := range n.Children {
		if c < 0 || c >= header.NrNodes+1 {
			panic(fmt.Sprintf("child out of range: %x[%x]=%x", n.Index, i, c))
		}
	}
	return n, nil
}

func (snapshot *Snapshot) writeNode(node *node) error {
	if dbg > 2 {
		fmt.Fprintf(os.Stdout, " write %x: %x\n", node.Index, node)
	}
	_, err := snapshot.file.Seek(int64(HEADER_SIZE+NODE_SIZE*(node.Index-1)), os.SEEK_SET)
	if err != nil {
		return err
	}
	dn := node.toDisk()
	err = binary.Write(snapshot.file, binary.LittleEndian, dn)
	if err != nil {
		return err
	}
	padding := NODE_SIZE - binary.Size(dn)
	_, err = snapshot.file.Write(make([]byte, padding))
	return err
}

func (snapshot *Snapshot) readHeader() (*header, error) {
	_, err := snapshot.file.Seek(0, os.SEEK_SET)
	if err != nil {
		return nil, err
	}
	header := new(header)
	err = binary.Read(snapshot.file, binary.LittleEndian, header)
	if err != nil {
		return nil, err
	}
	return header, nil
}

func (snapshot *Snapshot) writeHeader(header *header) error {
	_, err := snapshot.file.Seek(0, os.SEEK_SET)
	if err != nil {
		return err
	}
	return binary.Write(snapshot.file, binary.LittleEndian, header)
}

func (snapshot *Snapshot) allocNode() (int64, error) {
	snapshot.tree.allocMutex.Lock()
	defer snapshot.tree.allocMutex.Unlock()
	// read current number of nodes
	header, err := snapshot.readHeader()
	if err != nil {
		return -1, err
	}
	// get the new new node index
	header.NrNodes++
	newIndex := header.NrNodes
	// write back increased number of nodes
	err = snapshot.writeHeader(header)
	if err != nil {
		return -1, err
	}
	return newIndex, nil
}

// Note: doesn't write the node onto disk yet
func (snapshot *Snapshot) newNode(keyOffset int) (*node, error) {
	ix, err := snapshot.allocNode()
	if err != nil {
		return nil, err
	}
	n := &node{Index: ix, KeyOffset: keyOffset}
	return n, nil
}

type SiblingHash struct {
	Hash          []byte
	KeyOffset     int
	IsLeftSibling bool // whether the hashed sibling was to the left of the original node on the path
}

type LookupResult struct {
	leafData
	SiblingHashes []SiblingHash
}

func firstMismatch(slice1 []byte, slice2 []byte) int {
	shorterLen := min(len(slice1), len(slice2))
	for i := 0; i < shorterLen; i++ {
		if slice1[i] != slice2[i] {
			return i
		}
	}
	return shorterLen
}

// returns (nodes on matching path, position of last node, mismatch position in last node, error)
func (snapshot *Snapshot) partialLookup(key []byte) ([]*node, int, int, error) {
	if dbg > 1 {
		fmt.Fprintf(os.Stdout, "partialLookup(%x)\n", key)
	}
	if snapshot.Id == 0 {
		// no root, empty tree
		return nil, 0, 0, nil
	}
	root, err := snapshot.readNode(snapshot.Id, 0)
	if err != nil {
		return nil, 0, 0, err
	}
	nodes := []*node{root}
	n := root
	pos := 0
	for {
		mismatchPos := 0
		// First, compare the substring on the current n.
		if len(n.KeySubstring) > 0 {
			keySubstr := key[pos : pos+len(n.KeySubstring)]
			nodeSubstr := n.KeySubstring
			mismatchPos = firstMismatch(keySubstr, nodeSubstr)
			if mismatchPos != len(n.KeySubstring) {
				// Mismatch in the middle of the edge
				if dbg > 2 {
					fmt.Fprintf(os.Stdout, "partialLookup(%x) midsmatch %x+%x\n", key, pos, mismatchPos)
				}
				return nodes, pos, mismatchPos, nil
			}
			pos += len(n.KeySubstring)
			if pos == KEY_ELEMENTS {
				// Full match
				return nodes, KEY_ELEMENTS, 0, nil
			}
			if pos > KEY_ELEMENTS {
				return nil, 0, 0, &MapError{"corrupted tree: key too long"}
			}
		}
		// Then, index into the children
		childIx := key[pos]
		if n.Children[childIx] == 0 {
			// Mismatch at the end of the edge
			if dbg > 2 {
				fmt.Fprintf(os.Stdout, "partialLookup(%x) endsmatch %x\n", key, pos)
			}
			return nodes, pos - len(n.KeySubstring), len(n.KeySubstring), nil
		} else {
			pos++
			n, err = snapshot.readNode(n.Children[childIx], pos)
			nodes = append(nodes, n)
			if err != nil {
				return nil, 0, 0, err
			}
		}
	}
}

func (snapshot *Snapshot) GetPath(keyBytes *[KEY_BYTES]byte) (*LookupResult, error) {
	err := snapshot.openFile()
	if err != nil {
		return nil, err
	}
	defer snapshot.closeFile()
	key := bytesToElements(keyBytes[:])
	nodes, pos, _, err := snapshot.partialLookup(key)
	if err != nil {
		return nil, err
	}
	if len(nodes) == 0 || pos != KEY_ELEMENTS {
		return nil, nil
	}
	pathHashes := make([]SiblingHash, 0)
	for i, n := range nodes {
		for j := 0; j < ELEMENT_BITS; j++ {
			keyIx := n.KeyOffset + len(n.KeySubstring)
			if keyIx == KEY_ELEMENTS {
				break
			}
			elem := key[keyIx]
			hnIx := n.IndexHashNode(j, int(elem>>uint(ELEMENT_BITS-j)))
			hashNode := n.getHashNode(hnIx)
			if hashNode.HasTwoChildren {
				siblingSide := int(1 - ((elem >> uint(ELEMENT_BITS-j-1)) & 1))
				siblingNode := n.getHashNode(hnIx*2 + 1 + siblingSide)
				if dbg > 1 {
					side := 1 - siblingSide
					node := n.getHashNode(hnIx*2 + 1 + side)
					fmt.Fprintf(os.Stdout, "lookup %x/%x: %x %x %x !%4b[%x]=%x\n", keyBytes, i, hashNode, siblingNode, node, elem, j, siblingSide)
				}
				pathHashes = append(pathHashes, SiblingHash{
					KeyOffset:     n.KeyOffset + j,
					Hash:          siblingNode.Hash,
					IsLeftSibling: siblingSide == 0,
				})
			}
		}
	}
	result := &LookupResult{leafData: nodes[len(nodes)-1].leafData, SiblingHashes: pathHashes}
	err = snapshot.closeFile()
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (snapshot *Snapshot) updatePath(path []*node) error {
	for {
		ix, err := snapshot.allocNode()
		if err != nil {
			return err
		}
		n := path[len(path)-1]
		oldix := n.Index
		n.Index = ix
		snapshot.writeNode(n)
		if len(path) == 1 {
			snapshot.Id = ix
			return nil
		} else {
			parent := path[len(path)-2]
			setChild := false
			for i, child := range parent.Children {
				if child == oldix {
					parent.SetChild(i, n)
					setChild = true
					break
				}
			}
			if !setChild {
				return &MapError{"Inconsistent tree!"}
			}
			// continue with one node less
			path = path[:len(path)-1]
		}
	}
}

func (snapshot *Snapshot) Set(keyBytes *[KEY_BYTES]byte, value *[HASH_BYTES]byte) error {
	key := bytesToElements(keyBytes[:])

	data := &leafData{Value: *value, Hash: *Hash(append(keyBytes[:], value[:]...))}

	snapshot.openFile()
	defer snapshot.closeFile()

	nodes, pos, mismatchPos, err := snapshot.partialLookup(key)
	if err != nil {
		return err
	}
	if len(nodes) == 0 {
		// Create root node
		rootNode, err := snapshot.newNode(0)
		if err != nil {
			return err
		}
		rootNode.leafData = *data
		rootNode.KeySubstring = key
		err = snapshot.writeNode(rootNode)
		if err != nil {
			return err
		}
		snapshot.Id = rootNode.Index
		return nil
	} else {
		lastNode := nodes[len(nodes)-1]
		if pos == KEY_ELEMENTS {
			// Update leaf node
			if dbg > 1 {
				fmt.Fprintf(os.Stdout, " update at %v+%v\n", pos, mismatchPos)
			}
			lastNode.leafData = *data
		} else {
			// Make new child node
			newNode, err := snapshot.newNode(pos + mismatchPos + 1)
			if err != nil {
				return err
			}
			newNode.KeySubstring = key[pos+mismatchPos+1:]
			newNode.leafData = *data
			if mismatchPos == len(lastNode.KeySubstring) {
				if dbg > 1 {
					fmt.Fprintf(os.Stdout, " add at %v+%v\n", pos, mismatchPos)
				}
				lastNode.SetChild(int(key[pos+mismatchPos]), newNode)
			} else {
				if dbg > 1 {
					fmt.Fprintf(os.Stdout, " split at %v+%v\n", pos, mismatchPos)
				}
				// Split node: allocate second child node
				splitNode, err := snapshot.newNode(pos + mismatchPos + 1)
				if err != nil {
					return err
				}
				oldSubstr := lastNode.KeySubstring
				mismatchedSubstr := oldSubstr[mismatchPos+1 : len(lastNode.KeySubstring)]
				splitNode.KeySubstring = mismatchedSubstr

				splitNode.leafData = lastNode.leafData
				lastNode.leafData = leafData{}
				copy(splitNode.Children[:], lastNode.Children[:])
				copy(splitNode.ChildHashes[:], lastNode.ChildHashes[:])
				copy(lastNode.Children[:], make([]int64, NODE_CHILDREN))
				copy(lastNode.ChildHashes[:], make([][HASH_BYTES]byte, NODE_CHILDREN))

				assert(oldSubstr[mismatchPos] != key[pos+mismatchPos])
				lastNode.SetChild(int(oldSubstr[mismatchPos]), splitNode)
				lastNode.SetChild(int(key[pos+mismatchPos]), newNode)

				lastNode.KeySubstring = oldSubstr[:mismatchPos]

				err = snapshot.writeNode(splitNode)
				if err != nil {
					return err
				}
			}
			err = snapshot.writeNode(newNode)
			if err != nil {
				return err
			}
		}
		snapshot.updatePath(nodes)
	}
	err = snapshot.file.Sync()
	if err != nil {
		return err
	}
	return snapshot.closeFile()
}

func (snapshot *Snapshot) GetRootHash() (*[HASH_BYTES]byte, error) {
	if snapshot.Id == 0 {
		// zero nodes
		return nil, nil
	}
	snapshot.openFile()
	defer snapshot.closeFile()
	n, err := snapshot.readNode(snapshot.Id, 0)
	if err != nil {
		return nil, err
	}
	if len(n.KeySubstring) == KEY_ELEMENTS {
		// one node
		return &n.Hash, nil
	} else {
		// multiple nodes
		rootHash := new([HASH_BYTES]byte)
		copy(rootHash[:], n.getHashNode(n.IndexHashNode(0, 0)).Hash)
		return rootHash, nil
	}
}

func (lookup *LookupResult) ComputeRootHash() []byte {
	if dbg > 0 {
		fmt.Fprintf(os.Stdout, "computing root hash\n")
	}
	hash := lookup.Hash[:]
	if dbg > 0 {
		fmt.Fprintf(os.Stdout, "leaf hash: %x\n", hash)
	}
	for i := len(lookup.SiblingHashes) - 1; i >= 0; i-- {
		siblingHash := lookup.SiblingHashes[i]
		if siblingHash.IsLeftSibling {
			if dbg > 0 {
				fmt.Fprintf(os.Stdout, "left: ")
			}
			hash = Hash(append(siblingHash.Hash, hash...))[:]
		} else {
			if dbg > 0 {
				fmt.Fprintf(os.Stdout, "right: ")
			}
			hash = Hash(append(hash, siblingHash.Hash...))[:]
		}
		if dbg > 1 {
			fmt.Fprintf(os.Stdout, "new hash: %x\n", hash)
		}
	}
	return hash
}

type Snapshot struct {
	tree *Map
	file *os.File
	Id   int64
}

func (snapshot *Snapshot) openFile() error {
	if snapshot.file != nil {
		return &MapError{"Snapshot file already open"}
	}
	fi, err := os.OpenFile("tree.dat", os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	snapshot.file = fi
	return nil
}

func (snapshot *Snapshot) closeFile() error {
	if snapshot.file == nil {
		return nil
	}
	err := snapshot.file.Close()
	snapshot.file = nil
	return err
}

func (tree *Map) GetSnapshot(id int64) *Snapshot {
	return &Snapshot{tree: tree, Id: id}
}

