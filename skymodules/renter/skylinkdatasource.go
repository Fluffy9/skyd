package renter

import (
	"context"

	"gitlab.com/NebulousLabs/Sia/crypto"
	"gitlab.com/NebulousLabs/Sia/modules"
	"gitlab.com/NebulousLabs/Sia/types"
	"gitlab.com/SkynetLabs/skyd/build"
	"gitlab.com/SkynetLabs/skyd/skykey"
	"gitlab.com/SkynetLabs/skyd/skymodules"

	"gitlab.com/NebulousLabs/errors"
)

var (
	// skylinkDataSourceRequestSize is the size that is suggested by the data
	// source to be used when reading data from it.
	skylinkDataSourceRequestSize = build.Select(build.Var{
		Dev:      uint64(1 << 18), // 256 KiB
		Standard: uint64(1 << 20), // 1 MiB
		Testing:  uint64(1 << 9),  // 512 B
	}).(uint64)
)

type (
	// skylinkDataSource implements streamBufferDataSource on a Skylink.
	// Notably, it creates a pcws for every single chunk in the Skylink and
	// keeps them in memory, to reduce latency on seeking through the file.
	skylinkDataSource struct {
		// Metadata.
		staticID          skymodules.DataSourceID
		staticLayout      skymodules.SkyfileLayout
		staticMetadata    skymodules.SkyfileMetadata
		staticRawMetadata []byte

		// staticBaseSectorPayload will contain the raw data for the skylink
		// if there is no fanout. However if there's a fanout it will be nil.
		staticBaseSectorPayload []byte

		// staticChunkFetchers contains one pcws for every chunk in the fanout.
		// The worker sets are spun up in advance so that the HasSector queries
		// have completed by the time that someone needs to fetch the data.
		staticChunkFetchers []chunkFetcher

		// Utilities
		staticCtx        context.Context
		staticCancelFunc context.CancelFunc
		staticRenter     *Renter
	}
)

// DataSize implements streamBufferDataSource
func (sds *skylinkDataSource) DataSize() uint64 {
	return sds.staticLayout.Filesize
}

// ID implements streamBufferDataSource
func (sds *skylinkDataSource) ID() skymodules.DataSourceID {
	return sds.staticID
}

// Layout implements streamBufferDataSource
func (sds *skylinkDataSource) Layout() skymodules.SkyfileLayout {
	return sds.staticLayout
}

// Metadata implements streamBufferDataSource
func (sds *skylinkDataSource) Metadata() skymodules.SkyfileMetadata {
	return sds.staticMetadata
}

// RawMetadata implements streamBufferDataSource
func (sds *skylinkDataSource) RawMetadata() []byte {
	return sds.staticRawMetadata
}

// RequestSize implements streamBufferDataSource
func (sds *skylinkDataSource) RequestSize() uint64 {
	return skylinkDataSourceRequestSize
}

// SilentClose implements streamBufferDataSource
func (sds *skylinkDataSource) SilentClose() {
	// Cancelling the context for the data source should be sufficient. As all
	// child processes (such as the pcws for each chunk) should be using
	// contexts derived from the sds context.
	sds.staticCancelFunc()
}

// ReadStream implements streamBufferDataSource
func (sds *skylinkDataSource) ReadStream(ctx context.Context, off, fetchSize uint64, pricePerMS types.Currency) chan *readResponse {
	// Prepare the response channel
	responseChan := make(chan *readResponse, 1)
	if off+fetchSize > sds.staticLayout.Filesize {
		responseChan <- &readResponse{
			staticErr: errors.New("given offset and fetchsize exceed the underlying filesize"),
		}
		return responseChan
	}

	// If there's data in the base sector payload it means we are dealing with a
	// small skyfile without fanout bytes. This means we can simply read from
	// that and return early.
	baseSectorPayloadLen := uint64(len(sds.staticBaseSectorPayload))
	if baseSectorPayloadLen != 0 {
		bytesLeft := baseSectorPayloadLen - off
		if fetchSize > bytesLeft {
			fetchSize = bytesLeft
		}
		responseChan <- &readResponse{
			staticData: sds.staticBaseSectorPayload[off : off+fetchSize],
		}
		return responseChan
	}

	// Determine how large each chunk is.
	chunkSize := uint64(sds.staticLayout.FanoutDataPieces) * modules.SectorSize

	// Prepare an array of download chans on which we'll receive the data.
	numChunks := fetchSize / chunkSize
	if fetchSize%chunkSize != 0 {
		numChunks += 1
	}
	downloadChans := make([]chan *downloadResponse, 0, numChunks)

	// Otherwise we are dealing with a large skyfile and have to aggregate the
	// download responses for every chunk in the fanout. We keep reading from
	// chunks until all the data has been read.
	var n uint64
	for n < fetchSize && off < sds.staticLayout.Filesize {
		// Determine which chunk the offset is currently in.
		chunkIndex := off / chunkSize
		offsetInChunk := off % chunkSize
		remainingBytes := fetchSize - n

		// Determine how much data to read from the chunk.
		remainingInChunk := chunkSize - offsetInChunk
		downloadSize := remainingInChunk
		if remainingInChunk > remainingBytes {
			downloadSize = remainingBytes
		}

		// Schedule the download.
		respChan, err := sds.staticChunkFetchers[chunkIndex].Download(ctx, pricePerMS, offsetInChunk, downloadSize)
		if err != nil {
			responseChan <- &readResponse{
				staticErr: errors.AddContext(err, "unable to start download"),
			}
			return responseChan
		}
		downloadChans = append(downloadChans, respChan)

		off += downloadSize
		n += downloadSize
	}

	// Launch a goroutine that collects all download responses, aggregates them
	// and sends it as a single response over the response channel.
	err := sds.staticRenter.tg.Launch(func() {
		data := make([]byte, fetchSize)
		offset := 0
		failed := false

		for _, respChan := range downloadChans {
			resp := <-respChan
			if resp.err == nil {
				n := copy(data[offset:], resp.data)
				offset += n
				continue
			}
			if !failed {
				failed = true
				responseChan <- &readResponse{staticErr: resp.err}
				close(responseChan)
			}
		}

		if !failed {
			responseChan <- &readResponse{staticData: data}
			close(responseChan)
		}
	})
	if err != nil {
		responseChan <- &readResponse{staticErr: err}
	}
	return responseChan
}

// managedDownloadByRoot will fetch data using the merkle root of that data.
func (r *Renter) managedDownloadByRoot(ctx context.Context, root crypto.Hash, offset, length uint64, pricePerMS types.Currency) ([]byte, error) {
	// Create a context that dies when the function ends, this will cancel all
	// of the worker jobs that get created by this function.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Create the pcws for the first chunk. We use a passthrough cipher and
	// erasure coder. If the base sector is encrypted, we will notice and be
	// able to decrypt it once we have fully downloaded it and are able to
	// access the layout. We can make the assumption on the erasure coding being
	// of 1-N seeing as we currently always upload the basechunk using 1-N
	// redundancy.
	ptec := skymodules.NewPassthroughErasureCoder()
	tpsk, err := crypto.NewSiaKey(crypto.TypePlain, nil)
	if err != nil {
		return nil, errors.AddContext(err, "unable to create plain skykey")
	}
	pcws, err := r.newPCWSByRoots(ctx, []crypto.Hash{root}, ptec, tpsk, 0)
	if err != nil {
		return nil, errors.AddContext(err, "unable to create the worker set for this skylink")
	}

	// Download the base sector. The base sector contains the metadata, without
	// it we can't provide a completed data source.
	//
	// NOTE: we pass in the provided context here, if the user imposed a timeout
	// on the download request, this will fire if it takes too long.
	respChan, err := pcws.managedDownload(ctx, pricePerMS, offset, length)
	if err != nil {
		return nil, errors.AddContext(err, "unable to start download")
	}
	resp := <-respChan
	if resp.err != nil {
		return nil, errors.AddContext(resp.err, "base sector download did not succeed")
	}
	baseSector := resp.data
	if len(baseSector) < skymodules.SkyfileLayoutSize {
		return nil, errors.New("download did not fetch enough data, layout cannot be decoded")
	}

	return baseSector, nil
}

// managedSkylinkDataSource will create a streamBufferDataSource for the data
// contained inside of a Skylink. The function will not return until the base
// sector and all skyfile metadata has been retrieved.
//
// NOTE: Skylink data sources are cached and outlive the user's request because
// multiple different callers may want to use the same data source. We do have
// to pass in a context though to adhere to a possible user-imposed request
// timeout. This can be optimized to always create the data source when it was
// requested, but we should only do so after gathering some real world feedback
// that indicates we would benefit from this.
func (r *Renter) managedSkylinkDataSource(ctx context.Context, link skymodules.Skylink, pricePerMS types.Currency) (streamBufferDataSource, error) {
	// Get the offset and fetchsize from the skylink
	offset, fetchSize, err := link.OffsetAndFetchSize()
	if err != nil {
		return nil, errors.AddContext(err, "unable to parse skylink")
	}

	// Download the base sector. The base sector contains the metadata, without
	// it we can't provide a completed data source.
	//
	// NOTE: we pass in the provided context here, if the user imposed a timeout
	// on the download request, this will fire if it takes too long.
	baseSector, err := r.managedDownloadByRoot(ctx, link.MerkleRoot(), offset, fetchSize, pricePerMS)
	if err != nil {
		return nil, errors.AddContext(err, "unable to download base sector")
	}

	// Check if the base sector is encrypted, and attempt to decrypt it.
	// This will fail if we don't have the decryption key.
	var fileSpecificSkykey skykey.Skykey
	if skymodules.IsEncryptedBaseSector(baseSector) {
		fileSpecificSkykey, err = r.managedDecryptBaseSector(baseSector)
		if err != nil {
			return nil, errors.AddContext(err, "unable to decrypt skyfile base sector")
		}
	}

	// Parse out the metadata of the skyfile.
	layout, fanoutBytes, metadata, rawMetadata, baseSectorPayload, err := skymodules.ParseSkyfileMetadata(baseSector)
	if err != nil {
		return nil, errors.AddContext(err, "error parsing skyfile metadata")
	}

	// Create the context for the data source - a child of the renter
	// threadgroup but otherwise independent.
	dsCtx, cancelFunc := context.WithCancel(r.tg.StopCtx())

	// If there's a fanout create a PCWS for every chunk.
	var fanoutChunkFetchers []chunkFetcher
	if len(fanoutBytes) > 0 {
		// Derive the fanout key
		fanoutKey, err := skymodules.DeriveFanoutKey(&layout, fileSpecificSkykey)
		if err != nil {
			cancelFunc()
			return nil, errors.AddContext(err, "unable to derive encryption key")
		}

		// Create the erasure coder
		ec, err := skymodules.NewRSSubCode(int(layout.FanoutDataPieces), int(layout.FanoutParityPieces), crypto.SegmentSize)
		if err != nil {
			cancelFunc()
			return nil, errors.AddContext(err, "unable to derive erasure coding settings for fanout")
		}

		// Create a PCWS for every chunk
		fanoutChunks, err := layout.DecodeFanoutIntoChunks(fanoutBytes)
		if err != nil {
			cancelFunc()
			return nil, errors.AddContext(err, "error parsing skyfile fanout")
		}
		for i, chunk := range fanoutChunks {
			pcws, err := r.newPCWSByRoots(dsCtx, chunk, ec, fanoutKey, uint64(i))
			if err != nil {
				cancelFunc()
				return nil, errors.AddContext(err, "unable to create worker set for all chunk indices")
			}
			fanoutChunkFetchers = append(fanoutChunkFetchers, pcws)
		}
	}

	sds := &skylinkDataSource{
		staticID:          link.DataSourceID(),
		staticLayout:      layout,
		staticMetadata:    metadata,
		staticRawMetadata: rawMetadata,

		staticBaseSectorPayload: baseSectorPayload,
		staticChunkFetchers:     fanoutChunkFetchers,

		staticCtx:        dsCtx,
		staticCancelFunc: cancelFunc,
		staticRenter:     r,
	}
	return sds, nil
}
