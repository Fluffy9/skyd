package renter

import (
	"bytes"
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitlab.com/NebulousLabs/Sia/crypto"
	"gitlab.com/NebulousLabs/fastrand"
	"gitlab.com/SkynetLabs/skyd/skymodules"
	"go.sia.tech/siad/modules"
	"go.sia.tech/siad/types"
)

// mockProjectChunkWorkerSet is a mock object implementing the chunkFetcher
// interface
type mockProjectChunkWorkerSet struct {
	staticDownloadResponseChan chan *downloadResponse
	staticDownloadData         []byte
	staticErr                  error
}

// Download implements the chunkFetcher interface.
func (m *mockProjectChunkWorkerSet) Download(ctx context.Context, pricePerMS types.Currency, offset, length uint64) (chan *downloadResponse, error) {
	m.staticDownloadResponseChan <- &downloadResponse{
		data: m.staticDownloadData[offset : offset+length],
		err:  nil,
	}
	return m.staticDownloadResponseChan, m.staticErr
}

// newChunkFetcher returns a chunk fetcher.
func newChunkFetcher(data []byte, err error) chunkFetcher {
	responseChan := make(chan *downloadResponse, 1)
	return &mockProjectChunkWorkerSet{
		staticDownloadResponseChan: responseChan,
		staticDownloadData:         data,
		staticErr:                  err,
	}
}

// TestSkylinkDataSource is a unit test that verifies the behaviour of a
// SkylinkDataSource. Note that we are using mocked data, testing of the
// datasource with live PCWSs attached will happen through integration tests.
func TestSkylinkDataSource(t *testing.T) {
	t.Parallel()
	t.Run("small", testSkylinkDataSourceSmallFile)
	t.Run("large", testSkylinkDataSourceLargeFile)
}

// testSkylinkDataSourceSmallFile verifies we can read from a datasource for a
// small skyfile.
func testSkylinkDataSourceSmallFile(t *testing.T) {
	data := fastrand.Bytes(int(modules.SectorSize))
	datasize := uint64(len(data))

	ctx, cancel := context.WithCancel(context.Background())

	sds := &skylinkDataSource{
		staticID: skymodules.DataSourceID(crypto.Hash{1, 2, 3}),
		staticLayout: skymodules.SkyfileLayout{
			Version:            skymodules.SkyfileVersion,
			Filesize:           datasize,
			MetadataSize:       14e3,
			FanoutSize:         0,
			FanoutDataPieces:   1,
			FanoutParityPieces: 10,
			CipherType:         crypto.TypePlain,
		},
		staticMetadata: skymodules.SkyfileMetadata{
			Filename: "thisisafilename",
			Length:   datasize,
		},

		staticBaseSectorPayload: data,
		staticChunkFetchers:     make([]chunkFetcher, 0),

		staticCancelFunc: cancel,
		staticCtx:        ctx,
		staticRenter:     new(Renter),
	}

	if sds.DataSize() != datasize {
		t.Fatal("unexpected", sds.DataSize(), datasize)
	}
	if sds.ID() != skymodules.DataSourceID(crypto.Hash{1, 2, 3}) {
		t.Fatal("unexpected")
	}
	if !reflect.DeepEqual(sds.Metadata(), skymodules.SkyfileMetadata{
		Filename: "thisisafilename",
		Length:   datasize,
	}) {
		t.Fatal("unexpected")
	}
	if sds.RequestSize() != skylinkDataSourceRequestSize {
		t.Fatal("unexpected")
	}

	// verify invalid offset and length
	responseChan := sds.ReadStream(context.Background(), 1, modules.SectorSize, types.ZeroCurrency)
	select {
	case resp := <-responseChan:
		if resp == nil || resp.staticErr == nil {
			t.Fatal("unexpected")
		}
		if !strings.Contains(resp.staticErr.Error(), "given offset and fetchsize exceed the underlying filesize") {
			t.Fatal("unexpected")
		}
	case <-time.After(time.Second):
		t.Fatal("unexpected")
	}

	length := fastrand.Uint64n(datasize/4) + 1
	offset := fastrand.Uint64n(datasize - length)
	responseChan = sds.ReadStream(context.Background(), offset, length, types.ZeroCurrency)
	select {
	case resp := <-responseChan:
		if resp == nil || resp.staticErr != nil {
			t.Fatal("unexpected")
		}
		if !bytes.Equal(resp.staticData, data[offset:offset+length]) {
			t.Log("expected: ", data[offset:offset+length], len(data[offset:offset+length]))
			t.Log("actual:   ", resp.staticData, len(resp.staticData))
			t.Fatal("unexepected data")
		}
	case <-time.After(time.Second):
		t.Fatal("unexpected")
	}

	select {
	case <-sds.staticCtx.Done():
		t.Fatal("unexpected")
	case <-time.After(10 * time.Millisecond):
		sds.SilentClose()
	}
	select {
	case <-sds.staticCtx.Done():
	case <-time.After(10 * time.Millisecond):
		t.Fatal("unexpected")
	}
}

// testSkylinkDataSourceLargeFile verifies we can read from a datasource for a
// large skyfile.
func testSkylinkDataSourceLargeFile(t *testing.T) {
	fanoutChunk1 := fastrand.Bytes(int(modules.SectorSize))
	fanoutChunk2 := fastrand.Bytes(int(modules.SectorSize) / 2)
	allData := append(fanoutChunk1, fanoutChunk2...)
	datasize := uint64(len(allData))

	ctx, cancel := context.WithCancel(context.Background())

	sds := &skylinkDataSource{
		staticID: skymodules.DataSourceID(crypto.Hash{1, 2, 3}),
		staticLayout: skymodules.SkyfileLayout{
			Version:            skymodules.SkyfileVersion,
			Filesize:           datasize,
			MetadataSize:       14e3,
			FanoutSize:         75e3,
			FanoutDataPieces:   1,
			FanoutParityPieces: 10,
			CipherType:         crypto.TypePlain,
		},
		staticMetadata: skymodules.SkyfileMetadata{
			Filename: "thisisafilename",
			Length:   datasize,
		},

		staticBaseSectorPayload: make([]byte, 0),
		staticChunkFetchers: []chunkFetcher{
			newChunkFetcher(fanoutChunk1, nil),
			newChunkFetcher(fanoutChunk2, nil),
		},

		staticCancelFunc: cancel,
		staticCtx:        ctx,
		staticRenter:     new(Renter),
	}

	if sds.DataSize() != datasize {
		t.Fatal("unexpected", sds.DataSize(), datasize)
	}
	if sds.ID() != skymodules.DataSourceID(crypto.Hash{1, 2, 3}) {
		t.Fatal("unexpected")
	}
	if !reflect.DeepEqual(sds.Metadata(), skymodules.SkyfileMetadata{
		Filename: "thisisafilename",
		Length:   datasize,
	}) {
		t.Fatal("unexpected")
	}
	if sds.RequestSize() != skylinkDataSourceRequestSize {
		t.Fatal("unexpected")
	}

	length := fastrand.Uint64n(datasize/4) + 1
	offset := fastrand.Uint64n(datasize - length)

	responseChan := sds.ReadStream(context.Background(), offset, length, types.ZeroCurrency)
	select {
	case resp := <-responseChan:
		if resp == nil || resp.staticErr != nil {
			t.Fatal("unexpected", resp.staticErr)
		}
		if !bytes.Equal(resp.staticData, allData[offset:offset+length]) {
			t.Log("expected: ", allData[offset:offset+length], len(allData[offset:offset+length]), length)
			t.Log("actual:   ", resp.staticData, len(resp.staticData))
			t.Fatal("unexepected data")
		}
	case <-time.After(time.Second):
		t.Fatal("unexpected")
	}

	select {
	case <-sds.staticCtx.Done():
		t.Fatal("unexpected")
	case <-time.After(10 * time.Millisecond):
		sds.SilentClose()
	}
	select {
	case <-sds.staticCtx.Done():
	case <-time.After(10 * time.Millisecond):
		t.Fatal("unexpected")
	}
}
