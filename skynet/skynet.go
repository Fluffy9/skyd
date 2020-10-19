package skynet

import (
	"encoding/binary"
	"encoding/json"

	"gitlab.com/NebulousLabs/Sia/build"
	"gitlab.com/NebulousLabs/Sia/crypto"
	"gitlab.com/NebulousLabs/Sia/modules"
	"gitlab.com/NebulousLabs/errors"
)

const (
	// SkyfileLayoutSize describes the amount of space within the first sector
	// of a skyfile used to describe the rest of the skyfile.
	SkyfileLayoutSize = 99

	// SkyfileVersion establishes the current version for creating skyfiles.
	// The skyfile versions are different from the siafile versions.
	SkyfileVersion = 1

	// layoutKeyDataSize is the size of the key-data field in a skyfileLayout.
	layoutKeyDataSize = 64
)

// SkyfileLayout explains the layout information that is used for storing data
// inside of the skyfile. The SkyfileLayout always appears as the first bytes
// of the leading chunk.
type SkyfileLayout struct {
	Version            uint8
	Filesize           uint64
	MetadataSize       uint64
	FanoutSize         uint64
	FanoutDataPieces   uint8
	FanoutParityPieces uint8
	CipherType         crypto.CipherType
	KeyData            [layoutKeyDataSize]byte // keyData is incompatible with ciphers that need keys larger than 64 bytes
}

// Encode will return a []byte that has compactly encoded all of the layout
// data.
func (sl *SkyfileLayout) Encode() []byte {
	b := make([]byte, SkyfileLayoutSize)
	offset := 0
	b[offset] = sl.Version
	offset += 1
	binary.LittleEndian.PutUint64(b[offset:], sl.Filesize)
	offset += 8
	binary.LittleEndian.PutUint64(b[offset:], sl.MetadataSize)
	offset += 8
	binary.LittleEndian.PutUint64(b[offset:], sl.FanoutSize)
	offset += 8
	b[offset] = sl.FanoutDataPieces
	offset += 1
	b[offset] = sl.FanoutParityPieces
	offset += 1
	copy(b[offset:], sl.CipherType[:])
	offset += len(sl.CipherType)
	copy(b[offset:], sl.KeyData[:])
	offset += len(sl.KeyData)

	// Sanity check. If this check fails, encode() does not match the
	// SkyfileLayoutSize.
	if offset != SkyfileLayoutSize {
		build.Critical("layout size does not match the amount of data encoded")
	}
	return b
}

// Decode will take a []byte and load the layout from that []byte.
func (sl *SkyfileLayout) Decode(b []byte) {
	offset := 0
	sl.Version = b[offset]
	offset += 1
	sl.Filesize = binary.LittleEndian.Uint64(b[offset:])
	offset += 8
	sl.MetadataSize = binary.LittleEndian.Uint64(b[offset:])
	offset += 8
	sl.FanoutSize = binary.LittleEndian.Uint64(b[offset:])
	offset += 8
	sl.FanoutDataPieces = b[offset]
	offset += 1
	sl.FanoutParityPieces = b[offset]
	offset += 1
	copy(sl.CipherType[:], b[offset:])
	offset += len(sl.CipherType)
	copy(sl.KeyData[:], b[offset:])
	offset += len(sl.KeyData)

	// Sanity check. If this check fails, decode() does not match the
	// SkyfileLayoutSize.
	if offset != SkyfileLayoutSize {
		build.Critical("layout size does not match the amount of data decoded")
	}
}

// ParseSkyfileMetadata will pull the metadata (including layout and fanout) out
// of a skyfile.
func ParseSkyfileMetadata(baseSector []byte) (sl SkyfileLayout, fanoutBytes []byte, sm modules.SkyfileMetadata, baseSectorPayload []byte, err error) {
	// Sanity check - baseSector should not be more than modules.SectorSize.
	// Note that the base sector may be smaller in the event of a packed
	// skyfile.
	if uint64(len(baseSector)) > modules.SectorSize {
		build.Critical("parseSkyfileMetadata given a baseSector that is too large")
	}

	// Parse the layout.
	var offset uint64
	sl.Decode(baseSector)
	offset += SkyfileLayoutSize

	// Check the version.
	if sl.Version != 1 {
		return SkyfileLayout{}, nil, modules.SkyfileMetadata{}, nil, errors.New("unsupported skyfile version")
	}

	// Currently there is no support for skyfiles with fanout + metadata that
	// exceeds the base sector.
	if offset+sl.FanoutSize+sl.MetadataSize > uint64(len(baseSector)) || sl.FanoutSize > modules.SectorSize || sl.MetadataSize > modules.SectorSize {
		return SkyfileLayout{}, nil, modules.SkyfileMetadata{}, nil, errors.New("this version of siad does not support skyfiles with large fanouts and metadata")
	}

	// Parse the fanout.
	// NOTE: we copy the fanoutBytes instead of returning a slice into baseSector
	// because in PinSkylink the baseSector may be re-encrypted.
	fanoutBytes = make([]byte, sl.FanoutSize)
	copy(fanoutBytes, baseSector[offset:offset+sl.FanoutSize])
	offset += sl.FanoutSize

	// Parse the metadata.
	metadataSize := sl.MetadataSize
	err = json.Unmarshal(baseSector[offset:offset+metadataSize], &sm)
	if err != nil {
		return SkyfileLayout{}, nil, modules.SkyfileMetadata{}, nil, errors.AddContext(err, "unable to parse SkyfileMetadata from skyfile base sector")
	}
	offset += metadataSize

	// In version 1, the base sector payload is nil unless there is no fanout.
	if sl.FanoutSize == 0 {
		baseSectorPayload = baseSector[offset : offset+sl.Filesize]
	}

	return sl, fanoutBytes, sm, baseSectorPayload, nil
}
