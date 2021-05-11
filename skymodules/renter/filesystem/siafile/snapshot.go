package siafile

import (
	"bytes"
	"io"
	"io/ioutil"
	"math"
	"os"

	"gitlab.com/NebulousLabs/errors"

	"gitlab.com/SkynetLabs/skyd/skymodules"
	"go.sia.tech/siad/crypto"
)

type (
	// Snapshot is a snapshot of a SiaFile. A snapshot is a deep-copy and
	// can be accessed without locking at the cost of being a frozen readonly
	// representation of a siafile which only exists in memory.
	Snapshot struct {
		staticChunks      []Chunk
		staticFileSize    int64
		staticPieceSize   uint64
		staticErasureCode skymodules.ErasureCoder
		staticMasterKey   crypto.CipherKey
		staticMode        os.FileMode
		staticPubKeyTable []HostPublicKey
		staticSiaPath     skymodules.SiaPath
		staticLocalPath   string
		staticUID         SiafileUID
	}
)

// SnapshotReader is a helper type that allows reading a raw SiaFile from disk
// while keeping the file in memory locked.
type SnapshotReader struct {
	f  *os.File
	sf *SiaFile
}

// Close closes the underlying file.
func (sfr *SnapshotReader) Close() error {
	defer sfr.sf.mu.RUnlock()
	return sfr.f.Close()
}

// Read calls Read on the underlying file.
func (sfr *SnapshotReader) Read(b []byte) (int, error) {
	return sfr.f.Read(b)
}

// Stat returns the FileInfo of the underlying file.
func (sfr *SnapshotReader) Stat() (os.FileInfo, error) {
	return sfr.f.Stat()
}

// SnapshotReader creates a io.ReadCloser that can be used to read the raw
// Siafile from disk. Note that the underlying siafile holds a readlock until
// the SnapshotReader is closed, which means that no operations can be called to
// the underlying siafile which may cause it to grab a lock, because that will
// cause a deadlock.
//
// Operations which require grabbing a readlock on the underlying siafile are
// also not okay, because if some other thread has attempted to grab a writelock
// on the siafile, the readlock will block and then the Close() statement may
// never be reached for the SnapshotReader.
//
// TODO: Things upstream would be a lot easier if we could drop the requirement
// to hold a lock for the duration of the life of the snapshot reader.
func (sf *SiaFile) SnapshotReader() (*SnapshotReader, error) {
	// Lock the file.
	sf.mu.RLock()
	if sf.deleted {
		sf.mu.RUnlock()
		return nil, errors.AddContext(ErrDeleted, "can't copy deleted SiaFile")
	}
	// Open file.
	f, err := os.Open(sf.siaFilePath)
	if err != nil {
		sf.mu.RUnlock()
		return nil, err
	}
	return &SnapshotReader{
		sf: sf,
		f:  f,
	}, nil
}

// ChunkIndexByOffset will return the chunkIndex that contains the provided
// offset of a file and also the relative offset within the chunk. If the
// offset is out of bounds, chunkIndex will be equal to NumChunk().
func (s *Snapshot) ChunkIndexByOffset(offset uint64) (chunkIndex uint64, off uint64) {
	chunkIndex = offset / s.ChunkSize()
	off = offset % s.ChunkSize()
	return
}

// ChunkSize returns the size of a single chunk of the file.
func (s *Snapshot) ChunkSize() uint64 {
	return s.staticPieceSize * uint64(s.staticErasureCode.MinPieces())
}

// ErasureCode returns the erasure coder used by the file.
func (s *Snapshot) ErasureCode() skymodules.ErasureCoder {
	return s.staticErasureCode
}

// LocalPath returns the localPath used to repair the file.
func (s *Snapshot) LocalPath() string {
	return s.staticLocalPath
}

// MasterKey returns the masterkey used to encrypt the file.
func (s *Snapshot) MasterKey() crypto.CipherKey {
	return s.staticMasterKey
}

// Mode returns the FileMode of the file.
func (s *Snapshot) Mode() os.FileMode {
	return s.staticMode
}

// NumChunks returns the number of chunks the file consists of. This will
// return the number of chunks the file consists of even if the file is not
// fully uploaded yet.
func (s *Snapshot) NumChunks() uint64 {
	return uint64(len(s.staticChunks))
}

// Pieces returns all the pieces for a chunk in a slice of slices that contains
// all the pieces for a certain index.
func (s *Snapshot) Pieces(chunkIndex uint64) [][]Piece {
	// Return the pieces. Since the snapshot is meant to be used read-only, we
	// don't have to return a deep-copy here.
	return s.staticChunks[chunkIndex].Pieces
}

// PieceSize returns the size of a single piece of the file.
func (s *Snapshot) PieceSize() uint64 {
	return s.staticPieceSize
}

// SiaPath returns the SiaPath of the file.
func (s *Snapshot) SiaPath() skymodules.SiaPath {
	return s.staticSiaPath
}

// Size returns the size of the file.
func (s *Snapshot) Size() uint64 {
	return uint64(s.staticFileSize)
}

// UID returns the UID of the file.
func (s *Snapshot) UID() SiafileUID {
	return s.staticUID
}

// readlockChunks reads all chunks from the siafile within the range [min;max].
func (sf *SiaFile) readlockChunks(min, max int) ([]chunk, error) {
	// Copy chunks.
	chunks := make([]chunk, 0, sf.numChunks)
	for chunkIndex := 0; chunkIndex < sf.numChunks; chunkIndex++ {
		if chunkIndex < min || chunkIndex > max {
			chunks = append(chunks, chunk{Index: chunkIndex})
			continue
		}
		// Read chunk.
		c, err := sf.chunk(chunkIndex)
		if err != nil {
			return nil, err
		}
		// Handle complete partial chunk.
		chunks = append(chunks, c)
	}
	return chunks, nil
}

// readlockSnapshot creates a snapshot of the SiaFile.
func (sf *SiaFile) readlockSnapshot(sp skymodules.SiaPath, chunks []chunk) (*Snapshot, error) {
	mk := sf.staticMasterKey()

	// Copy PubKeyTable.
	pkt := make([]HostPublicKey, len(sf.pubKeyTable))
	copy(pkt, sf.pubKeyTable)

	// Figure out how much memory we need to allocate for the piece sets and
	// pieces.
	var numPieceSets, numPieces int
	for _, chunk := range chunks {
		numPieceSets += len(chunk.Pieces)
		for pieceIndex := range chunk.Pieces {
			numPieces += len(chunk.Pieces[pieceIndex])
		}
	}
	// Allocate all the piece sets and pieces at once.
	allPieceSets := make([][]Piece, numPieceSets)
	allPieces := make([]Piece, numPieces)

	// Copy chunks.
	exportedChunks := make([]Chunk, 0, len(chunks))
	for _, chunk := range chunks {
		// Handle full chunk
		pieces := allPieceSets[:len(chunk.Pieces)]
		allPieceSets = allPieceSets[len(chunk.Pieces):]
		for pieceIndex := range pieces {
			pieces[pieceIndex] = allPieces[:len(chunk.Pieces[pieceIndex])]
			allPieces = allPieces[len(chunk.Pieces[pieceIndex]):]
			for i, piece := range chunk.Pieces[pieceIndex] {
				pieces[pieceIndex][i] = Piece{
					HostPubKey: sf.hostKey(piece.HostTableOffset).PublicKey,
					MerkleRoot: piece.MerkleRoot,
				}
			}
		}
		exportedChunks = append(exportedChunks, Chunk{
			Pieces: pieces,
		})
	}
	// Get non-static metadata fields under lock.
	fileSize := sf.staticMetadata.FileSize
	mode := sf.staticMetadata.Mode
	uid := sf.staticMetadata.UniqueID
	localPath := sf.staticMetadata.LocalPath

	return &Snapshot{
		staticChunks:      exportedChunks,
		staticFileSize:    fileSize,
		staticPieceSize:   sf.staticMetadata.StaticPieceSize,
		staticErasureCode: sf.staticMetadata.staticErasureCode,
		staticMasterKey:   mk,
		staticMode:        mode,
		staticPubKeyTable: pkt,
		staticSiaPath:     sp,
		staticLocalPath:   localPath,
		staticUID:         uid,
	}, nil
}

// Snapshot creates a snapshot of the SiaFile.
func (sf *SiaFile) Snapshot(sp skymodules.SiaPath) (*Snapshot, error) {
	sf.mu.RLock()
	defer sf.mu.RUnlock()

	chunks, err := sf.readlockChunks(0, math.MaxInt32)
	if err != nil {
		return nil, err
	}
	return sf.readlockSnapshot(sp, chunks)
}

// SnapshotRange creates a snapshot of the Siafile over a specific range.
func (sf *SiaFile) SnapshotRange(sp skymodules.SiaPath, offset, length uint64) (*Snapshot, error) {
	sf.mu.RLock()
	defer sf.mu.RUnlock()

	minChunk := int(offset / sf.staticChunkSize())
	maxChunk := int((offset + length) / sf.staticChunkSize())
	maxChunkOffset := (offset + length) % sf.staticChunkSize()
	if maxChunk > 0 && maxChunkOffset == 0 {
		maxChunk--
	}

	chunks, err := sf.readlockChunks(minChunk, maxChunk)
	if err != nil {
		return nil, err
	}
	return sf.readlockSnapshot(sp, chunks)
}

// SnapshotFromReader reads a siafile from the specified reader and creates a
// snapshot from it.
func SnapshotFromReader(sp skymodules.SiaPath, r io.Reader) (*Snapshot, error) {
	d, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}
	sf, chunks, err := LoadSiaFileFromReaderWithChunks(bytes.NewReader(d), "", nil)
	if err != nil {
		return nil, err
	}
	return sf.readlockSnapshot(sp, chunks.chunks)
}
