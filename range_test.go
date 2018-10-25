package merkletree

import (
	"bytes"
	"encoding/hex"
	"hash"
	"reflect"
	"testing"

	"gitlab.com/NebulousLabs/fastrand"
	"golang.org/x/crypto/blake2b"
)

// bytesRoot is a helper function that calculates the Merkle root of b.
func bytesRoot(b []byte, h hash.Hash, leafSize int) []byte {
	root, err := ReaderRoot(bytes.NewReader(b), h, leafSize)
	if err != nil {
		// should be unreachable, since ReaderRoot only reports unexpected
		// errors returned by the supplied io.Reader, and bytes.Reader does
		// not return any such errors.
		panic(err)
	}
	return root
}

// A precalcSubtreeHasher wraps an underlying SubtreeHasher. It uses
// precalculated subtree roots where possible, only falling back to the
// underlying SubtreeHasher if needed.
type precalcSubtreeHasher struct {
	precalc     [][]byte
	subtreeSize int
	h           hash.Hash
	sh          SubtreeHasher
}

func (p *precalcSubtreeHasher) NextSubtreeRoot(n int) ([]byte, error) {
	if n%p.subtreeSize == 0 && len(p.precalc) >= n/p.subtreeSize {
		np := n / p.subtreeSize
		tree := New(p.h)
		for _, root := range p.precalc[:np] {
			tree.PushSubTree(0, root)
		}
		p.precalc = p.precalc[np:]
		return tree.Root(), p.sh.Skip(n)
	}
	return p.sh.NextSubtreeRoot(n)
}

func (p *precalcSubtreeHasher) Skip(n int) error {
	skippedHashes := n / p.subtreeSize
	if n%p.subtreeSize != 0 {
		skippedHashes++
	}
	p.precalc = p.precalc[skippedHashes:]
	return p.sh.Skip(n)
}

func newPrecalcSubtreeHasher(precalc [][]byte, subtreeSize int, h hash.Hash, sh SubtreeHasher) *precalcSubtreeHasher {
	return &precalcSubtreeHasher{
		precalc:     precalc,
		subtreeSize: subtreeSize,
		h:           h,
		sh:          sh,
	}
}

// TestBuildVerifyRangeProof tests the BuildRangeProof and VerifyRangeProof
// functions.
func TestBuildVerifyRangeProof(t *testing.T) {
	// test some known proofs
	blake, _ := blake2b.New256(nil)
	leafData := make([]byte, 1<<22)
	const leafSize = 64
	numLeaves := len(leafData) / 64
	// convenience functions
	leafHash := func(leaf []byte) []byte { return leafSum(blake, leaf) }
	nodeHash := func(left, right []byte) []byte { return nodeSum(blake, left, right) }

	proof, err := BuildRangeProof(0, numLeaves, NewSubtreeReader(bytes.NewReader(leafData), leafSize, blake))
	if err != nil {
		t.Fatal(err)
	} else if len(proof) != 0 {
		t.Error("BuildRangeProof constructed an incorrect proof for the entire sector")
	}

	proof, err = BuildRangeProof(0, 1, NewSubtreeReader(bytes.NewReader(leafData), leafSize, blake))
	if err != nil {
		t.Fatal(err)
	}
	root := leafHash(leafData[:leafSize])
	for i := range proof {
		root = nodeHash(root, proof[i])
	}
	if hex.EncodeToString(root) != "50ed59cecd5ed3ca9e65cec0797202091dbba45272dafa3faa4e27064eedd52c" {
		t.Error("BuildRangeProof constructed an incorrect proof for the first leaf")
	} else if !VerifyRangeProof(leafData[:leafSize], blake, leafSize, 0, 1, proof, root) {
		t.Error("VerifyRangeProof failed to verify a known correct proof")
	}

	proof, err = BuildRangeProof(numLeaves-1, numLeaves, NewSubtreeReader(bytes.NewReader(leafData), leafSize, blake))
	if err != nil {
		t.Fatal(err)
	}
	root = leafHash(leafData[len(leafData)-leafSize:])
	for i := range proof {
		root = nodeHash(proof[len(proof)-i-1], root)
	}
	if hex.EncodeToString(root) != "50ed59cecd5ed3ca9e65cec0797202091dbba45272dafa3faa4e27064eedd52c" {
		t.Error("BuildRangeProof constructed an incorrect proof for the last leaf")
	} else if !VerifyRangeProof(leafData[len(leafData)-leafSize:], blake, leafSize, numLeaves-1, numLeaves, proof, root) {
		t.Error("VerifyRangeProof failed to verify a known correct proof")
	}

	proof, err = BuildRangeProof(10, 11, NewSubtreeReader(bytes.NewReader(leafData), leafSize, blake))
	if err != nil {
		t.Fatal(err)
	}
	root = leafHash(leafData[10*leafSize:][:leafSize])
	root = nodeHash(root, proof[2])
	root = nodeHash(proof[1], root)
	root = nodeHash(root, proof[3])
	root = nodeHash(proof[0], root)
	for i := 4; i < len(proof); i++ {
		root = nodeHash(root, proof[i])
	}
	if hex.EncodeToString(root) != "50ed59cecd5ed3ca9e65cec0797202091dbba45272dafa3faa4e27064eedd52c" {
		t.Error("BuildRangeProof constructed an incorrect proof for a middle leaf")
	} else if !VerifyRangeProof(leafData[10*leafSize:11*leafSize], blake, leafSize, 10, 11, proof, root) {
		t.Error("VerifyRangeProof failed to verify a known correct proof")
	}

	// this is the largest possible proof
	midl, midr := numLeaves/2-1, numLeaves/2+1
	proof, err = BuildRangeProof(midl, midr, NewSubtreeReader(bytes.NewReader(leafData), leafSize, blake))
	if err != nil {
		t.Fatal(err)
	}
	left := leafHash(leafData[midl*leafSize:][:leafSize])
	for i := 0; i < len(proof)/2; i++ {
		left = nodeHash(proof[len(proof)/2-i-1], left)
	}
	right := leafHash(leafData[(midr-1)*leafSize:][:leafSize])
	for i := len(proof) / 2; i < len(proof); i++ {
		right = nodeHash(right, proof[i])
	}
	root = nodeHash(left, right)
	if hex.EncodeToString(root) != "50ed59cecd5ed3ca9e65cec0797202091dbba45272dafa3faa4e27064eedd52c" {
		t.Error("BuildRangeProof constructed an incorrect proof for worst-case inputs")
	} else if !VerifyRangeProof(leafData[midl*leafSize:midr*leafSize], blake, leafSize, midl, midr, proof, root) {
		t.Error("VerifyRangeProof failed to verify a known correct proof")
	}

	// test some random proofs against VerifyRangeProof
	for i := 0; i < 5; i++ {
		start := fastrand.Intn(numLeaves - 1)
		end := start + fastrand.Intn(numLeaves-start)
		proof, err := BuildRangeProof(start, end, NewSubtreeReader(bytes.NewReader(leafData), leafSize, blake))
		if err != nil {
			t.Fatal(err)
		}
		if !VerifyRangeProof(leafData[start*leafSize:end*leafSize], blake, leafSize, start, end, proof, root) {
			t.Errorf("BuildRangeProof constructed an incorrect proof for range %v-%v", start, end)
		}
	}

	// test a proof with precomputed inputs
	precalcRoots := [][]byte{
		bytesRoot(leafData[:len(leafData)/2], blake, leafSize),
		bytesRoot(leafData[len(leafData)/2:], blake, leafSize),
	}
	precalc := newPrecalcSubtreeHasher(precalcRoots, numLeaves/2, blake, NewSubtreeReader(bytes.NewReader(leafData), leafSize, blake))
	proof, err = BuildRangeProof(numLeaves-1, numLeaves, precalc)
	if err != nil {
		t.Fatal(err)
	}
	recalcProof, err := BuildRangeProof(numLeaves-1, numLeaves, NewSubtreeReader(bytes.NewReader(leafData), leafSize, blake))
	if !reflect.DeepEqual(proof, recalcProof) {
		t.Fatal("precalc failed")
	}

	// test malformed inputs
	if VerifyRangeProof(make([]byte, leafSize), blake, leafSize, 0, 1, nil, nil) {
		t.Error("VerifyRangeProof verified an incorrect proof")
	}
}

// TestBuildVerifyReaderRangeProof tests the BuildReaderRangeProof and
// VerifyReaderRangeProof functions.
func TestBuildVerifyReaderRangeProof(t *testing.T) {
	blake, _ := blake2b.New256(nil)
	for i := 0; i < 10; i++ {
	retry:
		leafSize := fastrand.Intn(1 << 8)
		if leafSize == 0 {
			goto retry
		}
		data := fastrand.Bytes(leafSize*4 + fastrand.Intn(1<<10))
		numLeaves := len(data) / leafSize
		if len(data)%leafSize != 0 {
			numLeaves++
		}
		start := fastrand.Intn(numLeaves - 1)
		end := start + fastrand.Intn(numLeaves-start)
		if start == end {
			goto retry
		}
		proof, err := BuildReaderRangeProof(bytes.NewReader(data), blake, leafSize, start, end)
		if err != nil {
			t.Fatal(err)
		}
		ok, err := VerifyReaderRangeProof(bytes.NewReader(data[start*leafSize:]), blake, leafSize, start, end, proof, bytesRoot(data, blake, leafSize))
		if err != nil {
			t.Fatal(err)
		} else if !ok {
			t.Fatal("VerifyReaderProof failed to verify proof produced by BuildReaderProof")
		}
	}
}

// BenchmarkBuildRangeProof benchmarks the performance of BuildRangeProof for
// various proof ranges.
func BenchmarkBuildRangeProof(b *testing.B) {
	blake, _ := blake2b.New256(nil)
	leafData := fastrand.Bytes(1 << 22)
	const leafSize = 64
	numLeaves := len(leafData) / 64

	benchRange := func(start, end int) func(*testing.B) {
		return func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _ = BuildRangeProof(start, end, NewSubtreeReader(bytes.NewReader(leafData), leafSize, blake))
			}
		}
	}

	b.Run("single", benchRange(0, 1))
	b.Run("half", benchRange(0, numLeaves/2))
	b.Run("mid", benchRange(numLeaves/2, 1+numLeaves/2))
	b.Run("full", benchRange(0, numLeaves-1))
}

// BenchmarkBuildRangeProof benchmarks the performance of BuildRangeProof for
// various proof ranges when a subset of the roots have been precalculated.
func BenchmarkBuildRangeProofPrecalc(b *testing.B) {
	blake, _ := blake2b.New256(nil)
	leafData := fastrand.Bytes(1 << 22)
	const leafSize = 64
	numLeaves := len(leafData) / 64
	root := bytesRoot(leafData, blake, leafSize)

	// precalculate nodes to depth 4
	precalcRoots := make([][]byte, 16)
	precalcSize := numLeaves / 16
	for i := range precalcRoots {
		precalcRoots[i] = bytesRoot(leafData[i*precalcSize*leafSize:][:precalcSize*leafSize], blake, leafSize)
	}

	benchRange := func(start, end int) func(*testing.B) {
		return func(b *testing.B) {
			precalc := newPrecalcSubtreeHasher(precalcRoots, precalcSize, blake, NewSubtreeReader(bytes.NewReader(leafData), leafSize, blake))
			b.ReportAllocs()
			proof, _ := BuildRangeProof(start, end, precalc)
			if !VerifyRangeProof(leafData[start*leafSize:end*leafSize], blake, leafSize, start, end, proof, root) {
				b.Fatal("precalculated roots are incorrect")
			}
			for i := 0; i < b.N; i++ {
				precalc = newPrecalcSubtreeHasher(precalcRoots, precalcSize, blake, NewSubtreeReader(bytes.NewReader(leafData), leafSize, blake))
				_, _ = BuildRangeProof(start, end, precalc)
			}
		}
	}

	b.Run("single", benchRange(numLeaves-1, numLeaves))
	b.Run("sixteenth", benchRange(numLeaves-numLeaves/16, numLeaves))
}

// BenchmarkVerifyRangeProof benchmarks the performance of VerifyRangeProof
// for various proof ranges.
func BenchmarkVerifyRangeProof(b *testing.B) {
	blake, _ := blake2b.New256(nil)
	leafData := fastrand.Bytes(1 << 22)
	const leafSize = 64
	numLeaves := len(leafData) / 64
	root := bytesRoot(leafData, blake, leafSize)

	benchRange := func(start, end int) func(*testing.B) {
		proof, _ := BuildRangeProof(start, end, NewSubtreeReader(bytes.NewReader(leafData), leafSize, blake))
		return func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = VerifyRangeProof(leafData[start*leafSize:end*leafSize], blake, leafSize, start, end, proof, root)
			}
		}
	}

	b.Run("single", benchRange(0, 1))
	b.Run("half", benchRange(0, numLeaves/2))
	b.Run("mid", benchRange(numLeaves/2, 1+numLeaves/2))
	b.Run("full", benchRange(0, numLeaves-1))
}
