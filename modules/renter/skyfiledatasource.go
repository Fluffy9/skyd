package renter

import (
	"context"
	"sync/atomic"

	"gitlab.com/NebulousLabs/Sia/crypto"
	"gitlab.com/NebulousLabs/Sia/modules"
	"gitlab.com/NebulousLabs/Sia/modules/renter/filesystem/siafile"
	"gitlab.com/NebulousLabs/Sia/skykey"
	"gitlab.com/NebulousLabs/Sia/types"

	"gitlab.com/NebulousLabs/errors"
)

// skylinkDataSource implements streamBufferDataSource on a Skylink. Notably, it
// creates a pcws for every single chunk in the Skylink and keeps them in
// memory, to reduce latency on seeking through the file.
type skylinkDataSource struct {
	// atomicClosed is an atomic variable to check if the data source has been
	// closed yet. This is primarily used to ensure that the cancelFunc is only
	// called once.
	atomicClosed uint64

	// Metadata.
	staticID       modules.DataSourceID
	staticLayout   skyfileLayout
	staticMetadata modules.SkyfileMetadata

	// The base sector contains all of the raw data for the skylink, and the
	// fanoutPCWS contains one pcws for every chunk in the fanout. The worker
	// sets are spun up in advance so that the HasSector queries have completed
	// by the time that someone needs to fetch the data.
	staticFirstChunk []byte
	staticFanoutPCWS []*projectChunkWorkerSet

	// Utilities
	staticCancelFunc context.CancelFunc
	staticCtx        context.Context
	staticRenter     *Renter
}

// DataSize implements streamBufferDataSource
func (sds *skylinkDataSource) DataSize() uint64 {
	return sds.staticLayout.filesize
}

// ID implements streamBufferDataSource
func (sds *skylinkDataSource) ID() modules.DataSourceID {
	return sds.staticID
}

// Metadata implements streamBufferDataSource
func (sds *skylinkDataSource) Metadata() modules.SkyfileMetadata {
	return sds.staticMetadata
}

// RequestSize implements streamBufferDataSource
func (sds *skylinkDataSource) RequestSize() uint64 {
	return 1 << 18 // 256 KiB
}

// SilentClose implements streamBufferDataSource
func (sds *skylinkDataSource) SilentClose() {
	// Check if SilentClose has already been called.
	swapped := atomic.CompareAndSwapUint64(&sds.atomicClosed, 0, 1)
	if !swapped {
		return // already closed
	}

	// Cancelling the context for the data source should be sufficient. as all
	// child processes (such as the pcws for each chunk) should be using
	// contexts derived from the sds context.
	sds.staticCancelFunc()
}

// ReadAt implements streamBufferDataSource
//
// TODO: Adjust the interface so that ReadAt returns a channel instead of the
// full data, and so that it takes a pricePerMs as input. The channel allows the
// stream buffer to queue data more intelligently - the channel doesn't return
// until the downloads have been queued, giving the stream buffer control over
// what approximate order the data is returned.
func (sds *skylinkDataSource) ReadAt(p []byte, off int64) (n int, err error) {
	// TODO: Get this as input.
	pricePerMs := types.SiacoinPrecision

	// Determine if the first part of the data needs to be read from the first
	// chunk.
	if off < int64(len(sds.staticFirstChunk)) {
		n = copy(p, sds.staticFirstChunk[off:])
		off += int64(n)
	}

	// Determine how large each chunk is.
	chunkSize := uint64(sds.staticLayout.fanoutDataPieces) * modules.SectorSize

	// Keep reading from chunks until all the data has been read.
	off -= int64(len(sds.staticFirstChunk)) // Ignore data in the first chunk.
	for n < len(p) && off < int64(sds.staticLayout.filesize) {
		// Determine which chunk the offset is currently in.
		chunkIndex := uint64(off) / chunkSize
		offsetInChunk := uint64(off) % chunkSize
		remainingBytes := uint64(len(p) - n)

		// Determine how much data to read from the chunk.
		remainingInChunk := chunkSize - offsetInChunk
		downloadSize := remainingInChunk
		if remainingInChunk > remainingBytes {
			downloadSize = remainingBytes
		}

		// Issue the download.
		//
		// TODO: That pricePerMs needs to come from somewhere.
		respChan, err := sds.staticFanoutPCWS[chunkIndex].managedDownload(sds.staticCtx, pricePerMs, offsetInChunk, downloadSize)
		if err != nil {
			return n, errors.AddContext(err, "unable to start download")
		}
		resp := <-respChan
		if resp.err != nil {
			return n, errors.AddContext(err, "base sector download did not succeed")
		}
		m := copy(p[n:], resp.data)
		off += int64(m)
		n += m
	}
	return n, nil
}

// skylinkDataSource will create a streamBufferDataSource for the data contained
// inside of a Skylink. The function will not return until the base sector and
// all skyfile metadata has been retrieved.
//
// NOTE: Because multiple different callers may want to use the same data
// source, we want the data source to outlive the initial call. That is why
// there is no input for a context - the data source will live as long as the
// stream buffer determines is appropriate.
func (r *Renter) skylinkDataSource(link modules.Skylink, pricePerMs types.Currency) (streamBufferDataSource, error) {
	// Create the context for the data source - a child of the renter
	// threadgroup but otherwise independent.
	ctx, cancelFunc := context.WithCancel(r.tg.StopCtx())

	// Create the pcws for the first chunk, which is just a single root with
	// both passthrough encryption and passthrough erasure coding.
	ptec := modules.NewPassthroughErasureCoder()
	tpsk, err := crypto.NewSiaKey(crypto.TypePlain, nil)
	if err != nil {
		return nil, errors.AddContext(err, "unable to create plain skykey")
	}
	pcws, err := r.newPCWSByRoots(ctx, []crypto.Hash{link.MerkleRoot()}, ptec, tpsk, 0)
	if err != nil {
		return nil, errors.AddContext(err, "unable to create the worker set for this skylink")
	}

	// Download the base sector. The base sector contains the metadata, without
	// it we can't provide a completed data source.
	offset, fetchSize, err := link.OffsetAndFetchSize()
	if err != nil {
		return nil, errors.AddContext(err, "unable to parse skylink")
	}
	respChan, err := pcws.managedDownload(ctx, pricePerMs, offset, fetchSize)
	if err != nil {
		return nil, errors.AddContext(err, "unable to start download")
	}
	resp := <-respChan
	if resp.err != nil {
		return nil, errors.AddContext(err, "base sector download did not succeed")
	}
	baseSector := resp.data
	if len(baseSector) < SkyfileLayoutSize {
		return nil, errors.New("download did not fetch enough data, layout cannot be decoded")
	}

	// Check if the base sector is encrypted, and attempt to decrypt it.
	// This will fail if we don't have the decryption key.
	var fileSpecificSkykey skykey.Skykey
	if isEncryptedBaseSector(baseSector) {
		fileSpecificSkykey, err = r.decryptBaseSector(baseSector)
		if err != nil {
			return nil, errors.AddContext(err, "unable to decrypt skyfile base sector")
		}
	}

	// Parse out the metadata of the skyfile.
	layout, fanoutBytes, metadata, firstChunk, err := parseSkyfileMetadata(baseSector)
	if err != nil {
		return nil, errors.AddContext(err, "error parsing skyfile metadata")
	}

	// Determine the total number of chunks that are in the file.
	chunkSize := uint64(layout.fanoutDataPieces) * modules.SectorSize
	chunks := layout.filesize / chunkSize
	if layout.filesize%chunkSize != 0 {
		chunks++
	}

	// Grab the encryption key and the erasure coding parameters.
	masterKey, err := r.deriveFanoutKey(&layout, fileSpecificSkykey)
	if err != nil {
		return nil, errors.AddContext(err, "unable to derive encryption key")
	}
	ec, err := siafile.NewRSSubCode(int(layout.fanoutDataPieces), int(layout.fanoutParityPieces), crypto.SegmentSize)
	if err != nil {
		return nil, errors.AddContext(err, "unable to derive erasure coding settings for fanout")
	}

	// Build the pcws for each chunk.
	fanoutPCWS := make([]*projectChunkWorkerSet, chunks)
	fanoutOffset := 0
	for i := uint64(0); i < chunks; i++ {
		// Get the roots for this chunk.
		chunkRoots := make([]crypto.Hash, layout.fanoutDataPieces+layout.fanoutParityPieces)
		for j := 0; j < len(chunkRoots); j++ {
			copy(chunkRoots[j][:], fanoutBytes[fanoutOffset:])
			fanoutOffset += crypto.HashSize
		}
		pcws, err := r.newPCWSByRoots(ctx, chunkRoots, ec, masterKey, i)
		if err != nil {
			return nil, errors.AddContext(err, "unable to create worker set for all chunk indices")
		}
		fanoutPCWS[i] = pcws
	}

	sds := &skylinkDataSource{
		staticID:       link.DataSourceID(),
		staticLayout:   layout,
		staticMetadata: metadata,

		staticFirstChunk: firstChunk,
		staticFanoutPCWS: fanoutPCWS,

		staticCancelFunc: cancelFunc,
		staticCtx:        ctx,
		staticRenter:     r,
	}
	return sds, nil
}
