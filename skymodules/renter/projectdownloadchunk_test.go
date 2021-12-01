package renter

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"gitlab.com/NebulousLabs/errors"
	"gitlab.com/NebulousLabs/fastrand"
	"gitlab.com/SkynetLabs/skyd/skymodules"
	"go.sia.tech/siad/crypto"
	"go.sia.tech/siad/modules"
	"go.sia.tech/siad/types"
)

// TestPDC is a collection of unit test that verify the functionality of the
// project download chunk object.
func TestPDC(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	t.Parallel()

	t.Run("handleJobResponse", testProjectDownloadChunkHandleJobResponse)
	t.Run("finalize", testProjectDownloadChunkFinalize)
	t.Run("finished", testProjectDownloadChunkFinished)
	t.Run("launchWorker", testProjectDownloadChunkLaunchWorker)
	t.Run("workers", testProjectDownloadChunkWorkers)
}

// testProjectDownloadChunkHandleJobResponse is a unit test that verifies the
// functionality of the 'handleJobResponse' function on the ProjectDownloadChunk
func testProjectDownloadChunkHandleJobResponse(t *testing.T) {
	t.Parallel()

	// create pcws
	pcws := newTestProjectChunkWorkerSet()
	ec := pcws.staticErasureCoder

	// create data and erasure code
	data := fastrand.Bytes(int(modules.SectorSize))
	pieces, err := ec.Encode(data)
	if err != nil {
		t.Fatal(err)
	}

	// update piece roots
	empty := crypto.Hash{}
	pcws.staticPieceRoots = []crypto.Hash{
		empty,
		crypto.MerkleRoot(pieces[1]),
		empty,
		empty,
		empty,
	}

	// create pdc
	pdc := newTestProjectDownloadChunk(pcws, nil)
	pdc.piecesInfo[1].available++
	pdc.piecesInfo[2].available++

	// create worker
	worker := new(worker)

	// mock state after launching a worker
	workerKey := worker.staticHostPubKey.ShortString()
	pdc.workerProgressMap[workerKey] = workerProgress{
		completedPieces: make(completedPieces),
		launchedPieces:  make(launchedPieces),
	}

	lwi := &launchedWorkerInfo{staticLaunchTime: time.Now().Add(-time.Minute)}
	pdc.launchedWorkers = []*launchedWorkerInfo{lwi}

	// mock a successful read response for piece 1
	success := &jobReadResponse{
		staticData:    pieces[1],
		staticErr:     nil,
		staticJobTime: time.Duration(1),
		staticMetadata: jobReadMetadata{
			staticLaunchedWorkerIndex: 0,
			staticPieceRootIndex:      1,
			staticSectorRoot:          crypto.MerkleRoot(pieces[1]),
			staticWorker:              worker,
		},
	}
	pdc.handleJobReadResponse(success)

	// assert pieces info got updated
	if !pdc.piecesInfo[1].downloaded {
		t.Fatal("unexpected")
	}
	if pdc.piecesInfo[1].available != 0 {
		t.Fatal("unexpected")
	}

	// assert pieces data got updated and that we've unset the data
	if !bytes.Equal(pdc.piecesData[1], pieces[1]) {
		t.Fatal("unexpected")
	}
	if success.staticData != nil {
		t.Fatal("unexpected")
	}

	// assert the launched worker information got updated
	if lwi.completeTime == (time.Time{}) ||
		lwi.jobDuration == 0 ||
		lwi.totalDuration == 0 ||
		lwi.jobErr != nil {
		t.Fatal("unexpected")
	}

	// mock a failed read response for piece 2
	pdc.handleJobReadResponse(&jobReadResponse{
		staticData:    nil,
		staticErr:     errors.New("read failed"),
		staticJobTime: time.Duration(1),
		staticMetadata: jobReadMetadata{
			staticPieceRootIndex: 2,
			staticSectorRoot:     empty,
			staticWorker:         worker,
		},
	})

	// assert pieces info got updated
	if pdc.piecesInfo[2].downloaded {
		t.Fatal("unexpected")
	}
	if pdc.piecesInfo[2].available != 0 {
		t.Fatal("unexpected")
	}

	// assert pieces data
	if pdc.piecesData[2] != nil {
		t.Fatal("unexpected")
	}

	// assert the launched worker information got updated
	if lwi.completeTime == (time.Time{}) ||
		lwi.jobDuration == 0 ||
		lwi.totalDuration == 0 ||
		lwi.jobErr == nil {
		t.Fatal("unexpected", lwi)
	}
}

// testProjectDownloadChunkFinalize is a unit test for the 'finalize' function
// on the pdc. It verifies whether the returned data is properly offset to
// include only the pieces requested by the user.
func testProjectDownloadChunkFinalize(t *testing.T) {
	t.Parallel()

	// create PCWS
	pcws := newTestProjectChunkWorkerSet()
	ec := pcws.staticErasureCoder

	// create data
	originalData := fastrand.Bytes(int(modules.SectorSize))
	sectorRoot := crypto.MerkleRoot(originalData)
	pcws.staticPieceRoots = []crypto.Hash{sectorRoot}

	// RS encode the data
	data := make([]byte, modules.SectorSize)
	copy(data, originalData)
	pieces, err := ec.Encode(data)
	if err != nil {
		t.Fatal(err)
	}

	// download a random amount of data at random offset
	length := (fastrand.Uint64n(5) + 1) * crypto.SegmentSize
	offset := fastrand.Uint64n(modules.SectorSize - length)
	pieceOffset, pieceLength := getPieceOffsetAndLen(ec, offset, length)

	sliced := make([][]byte, len(pieces))
	for i, piece := range pieces {
		sliced[i] = make([]byte, pieceLength)
		copy(sliced[i], piece[pieceOffset:pieceOffset+pieceLength])
	}

	// create a pdc
	responseChan := make(chan *downloadResponse, 1)
	pdc := newTestProjectDownloadChunk(pcws, responseChan)
	pdc.offsetInChunk = offset
	pdc.lengthInChunk = length
	pdc.pieceOffset = pieceOffset
	pdc.pieceLength = pieceLength
	pdc.piecesData = sliced

	pdc.launchedWorkers = append(pdc.launchedWorkers, &launchedWorkerInfo{
		staticLaunchTime:           time.Now(),
		staticExpectedCompleteTime: time.Now().Add(time.Minute),
		staticExpectedDuration:     time.Minute,

		staticPDC:    pdc,
		staticWorker: new(worker),
	})

	// call finalize
	pdc.finalize()

	// verify the download
	downloadResponse := <-responseChan
	if downloadResponse.err != nil {
		t.Fatal("unexpected error", downloadResponse.err)
	}
	if !bytes.Equal(downloadResponse.data, originalData[offset:offset+length]) {
		t.Log("offset", offset)
		t.Log("length", length)
		t.Log("bytes downloaded", len(downloadResponse.data))

		t.Log("actual:\n", downloadResponse.data)
		t.Log("expected:\n", originalData[offset:offset+length])
		t.Fatal("unexpected data")
	}
	if downloadResponse.launchedWorkers == nil || len(downloadResponse.launchedWorkers) != 1 || downloadResponse.launchedWorkers[0].staticExpectedDuration != time.Minute {
		t.Fatal("unexpected")
	}

	// call fail
	pdc.fail(errors.New("failure"))
	downloadResponse = <-responseChan
	if downloadResponse.err == nil {
		t.Fatal("unexpected error")
	}
	if downloadResponse.launchedWorkers == nil {
		t.Fatal("unexpected")
	}
}

// testProjectDownloadChunkFinished is a unit test for the 'finished' function
// on the pdc. It verifies whether the hopeful and completed pieces are properly
// counted and whether the return values are correct.
func testProjectDownloadChunkFinished(t *testing.T) {
	// create EC
	ec, err := skymodules.NewRSCode(3, 5)
	if err != nil {
		t.Fatal("unexpected")
	}

	// create pdc
	pcws := newCustomTestProjectChunkWorkerSet(ec)
	pdc := newTestProjectDownloadChunk(pcws, nil)

	// mock unresolved state with hope of successful download
	pdc.unresolvedWorkersRemaining = 4
	finished, err := pdc.finished()
	if err != nil {
		t.Fatal("unexpected error", err)
	}
	if finished {
		t.Fatal("unexpected")
	}

	// mock one resolved piece - still unfinished but hopeful
	pdc.unresolvedWorkersRemaining = 3
	pdc.piecesInfo[0].available++
	finished, err = pdc.finished()
	if err != nil {
		t.Fatal("unexpected error", err)
	}
	if finished {
		t.Fatal("unexpected")
	}

	// mock all resolved, only 2 availables - should not be hopeful, need 3
	pdc.unresolvedWorkersRemaining = 0
	pdc.piecesInfo[1].available++
	finished, err = pdc.finished()
	if !errors.Contains(err, errNotEnoughPieces) {
		t.Fatal("unexpected error", err)
	}
	if finished {
		t.Fatal("unexpected")
	}

	// add one available - should be hopeful and unfinished
	pdc.piecesInfo[2].available++
	finished, err = pdc.finished()
	if err != nil {
		t.Fatal("unexpected error", err)
	}
	if finished {
		t.Fatal("unexpected")
	}

	// mock all downloaded, should be finished
	pdc.piecesInfo[0].available = 0
	pdc.piecesInfo[0].downloaded = true
	pdc.piecesInfo[1].available = 0
	pdc.piecesInfo[1].downloaded = true
	pdc.piecesInfo[2].available = 0
	pdc.piecesInfo[2].downloaded = true
	finished, err = pdc.finished()
	if err != nil {
		t.Fatal("unexpected error", err)
	}
	if !finished {
		t.Fatal("unexpected")
	}
}

// testProjectDownloadChunkLaunchWorker is a unit test for the 'launchWorker'
// function on the pdc.
func testProjectDownloadChunkLaunchWorker(t *testing.T) {
	t.Parallel()

	// mock a worker, ensure the readqueue returns a non zero time estimate
	worker := mockWorker(100 * time.Millisecond)
	workerIdentifier := worker.staticHostPubKey.ShortString()
	workerHostPubKeyStr := worker.staticHostPubKeyStr

	// create pdc
	pcws := newTestProjectChunkWorkerSet()
	pdc := newTestProjectDownloadChunk(pcws, nil)
	pdc.pieceLength = 1 << 16 // 64kb

	// launch a worker and expect it to have enqueued a job and expect the
	// complete time to be somewhere in the future
	expectedCompleteTime, added := pdc.launchWorker(&individualWorker{
		staticWorker:     worker,
		staticIdentifier: workerIdentifier,
	}, 0, false)
	if !added {
		t.Fatal("unexpected")
	}
	if expectedCompleteTime.Before(time.Now()) {
		t.Fatal("unexpected")
	}

	// assert worker progress has been initialised
	progress, exists := pdc.workerProgressMap[workerIdentifier]
	if !exists {
		t.Fatal("unexpected")
	}

	// verify one worker was launched without failure
	launchTime := progress.launchedPieces[0]
	if launchTime.IsZero() {
		t.Fatal("unexpected")
	}

	// mention of the launched worker should be present in the PDC's launched
	// worker map, which holds debug information about all workers that were
	// launched.
	if len(pdc.launchedWorkers) != 1 {
		t.Fatal("unexpected")
	}
	lw := pdc.launchedWorkers[0]

	// assert the launched worker info contains what we expect it to contain
	if lw.staticLaunchTime == (time.Time{}) ||
		lw.completeTime != (time.Time{}) ||
		lw.staticExpectedCompleteTime == (time.Time{}) ||
		lw.jobDuration != 0 ||
		lw.totalDuration != 0 ||
		lw.staticExpectedDuration == 0 ||
		!bytes.Equal(lw.staticPDC.uid[:], pdc.uid[:]) ||
		lw.staticWorker.staticHostPubKeyStr != workerHostPubKeyStr {
		t.Fatal("unexpected")
	}
}

// testProjectDownloadChunkWorkers is a unit test for the 'workers' function on
// the pdc.
func testProjectDownloadChunkWorkers(t *testing.T) {
	t.Parallel()

	// create pdc
	pcws := newTestProjectChunkWorkerSet()
	pdc := newTestProjectDownloadChunk(pcws, nil)
	ws := pdc.workerState

	// assert there are no workers
	workers := pdc.workers()
	if len(workers) != 0 {
		t.Fatal("bad")
	}

	// mock some workers
	w1 := mockWorker(0)
	w2 := mockWorker(0)
	w3 := mockWorker(0)

	// mock two unresolved workers
	ws.unresolvedWorkers["w1"] = &pcwsUnresolvedWorker{staticWorker: w1}
	ws.unresolvedWorkers["w2"] = &pcwsUnresolvedWorker{staticWorker: w2}

	// assert they're returned in the worker list
	workers = pdc.workers()
	if len(workers) != 2 {
		t.Fatal("bad")
	}

	// mock a resolved worker
	ws.resolvedWorkers = append(ws.resolvedWorkers, &pcwsWorkerResponse{
		worker:       w3,
		pieceIndices: []uint64{0},
	})

	// assert they're returned in the worker list
	workers = pdc.workers()
	if len(workers) != 3 {
		t.Fatal("bad")
	}

	// clear its piece indices and assert the worker is excluded
	ws.resolvedWorkers[0].pieceIndices = nil
	workers = pdc.workers()
	if len(workers) != 2 {
		t.Fatal("bad")
	}

	// mock w1 being on maintenance cooldown and assert the worker is excluded
	w1.staticMaintenanceState.cooldownUntil = time.Now().Add(time.Minute)
	workers = pdc.workers()
	if len(workers) != 1 {
		t.Fatal("bad")
	}
}

// TestGetPieceOffsetAndLen is a unit test that probes the helper function
// getPieceOffsetAndLength
func TestGetPieceOffsetAndLen(t *testing.T) {
	randOff := fastrand.Uint64n(modules.SectorSize)
	randLen := fastrand.Uint64n(modules.SectorSize)

	// verify an EC that does not support partials, defaults to a segemnt size
	// that is equal to the sectorsize
	ec := skymodules.NewRSCodeDefault()
	pieceOff, pieceLen := getPieceOffsetAndLen(ec, randOff, randLen)
	if pieceOff != 0 || pieceLen%modules.SectorSize != 0 {
		t.Fatal("unexpected", pieceOff, pieceLen)
	}

	// verify an EC that does support partials using the appropriate segment
	// size and the offset are as we expect them to be
	ec = skymodules.NewRSSubCodeDefault()
	pieceOff, pieceLen = getPieceOffsetAndLen(ec, randOff, randLen)
	if pieceOff%crypto.SegmentSize != 0 || pieceLen%crypto.SegmentSize != 0 {
		t.Fatal("unexpected", pieceOff, pieceLen)
	}

	// verify an EC with minPieces different from 1 that supports partials
	// encoding ensures we are reading enough data
	dataPieces := 2
	segmentSize := crypto.SegmentSize
	chunkSegmentSize := uint64(dataPieces * segmentSize)
	ec, err := skymodules.NewRSSubCode(2, 5, uint64(segmentSize))
	if err != nil {
		t.Fatal(err)
	}
	pieceOff, pieceLen = getPieceOffsetAndLen(ec, randOff, randLen)
	if ((pieceOff+pieceLen)*uint64(ec.MinPieces()))%chunkSegmentSize != 0 {
		t.Fatal("unexpected", pieceOff, pieceLen)
	}

	// verify an EC that returns a segment size of 0 is considered invalid
	ec = &mockErasureCoder{}
	defer func() {
		if r := recover(); r == nil || !strings.Contains(fmt.Sprintf("%v", r), "pcws has a bad erasure coder") {
			t.Fatal("Expected build.Critical", r)
		}
	}()
	getPieceOffsetAndLen(ec, 0, 0)
}

// TestGetPieceOffsetAndLenWithRecover is a unit test that isolates both
// 'getPieceOffsetAndLen' in combination with the Recover function on the EC and
// asserts we can properly encode and then recover at random offset and length
func TestGetPieceOffsetAndLenWithRecover(t *testing.T) {
	t.Parallel()

	// create data
	cntr := 0
	originalData := make([]byte, modules.SectorSize)
	for i := 0; i < int(modules.SectorSize); i += 2 {
		binary.BigEndian.PutUint16(originalData[i:], uint16(cntr))
		cntr += 1
	}

	// RS encode the data
	data := make([]byte, modules.SectorSize)
	copy(data, originalData)
	ec := skymodules.NewRSSubCodeDefault()
	pieces, err := ec.Encode(data)
	if err != nil {
		t.Fatal(err)
	}

	// Declare helper for testing.
	run := func(offset, length uint64) {
		pieceOffset, pieceLength := getPieceOffsetAndLen(ec, offset, length)
		skipLength := offset % (crypto.SegmentSize * uint64(ec.MinPieces()))

		sliced := make([][]byte, len(pieces))
		for i, piece := range pieces {
			sliced[i] = make([]byte, pieceLength)
			copy(sliced[i], piece[pieceOffset:pieceOffset+pieceLength])
		}

		buf := bytes.NewBuffer(nil)
		skipWriter := &skipWriter{
			writer: buf,
			skip:   int(skipLength),
		}
		err = ec.Recover(sliced, length+uint64(skipLength), skipWriter)
		if err != nil {
			t.Fatal(err)
		}
		actual := buf.Bytes()

		expected := originalData[offset : offset+length]
		if !bytes.Equal(actual, expected) {
			t.Log("Input       :", offset, length, pieceOffset, pieceLength)
			t.Log("original    :", originalData[:crypto.SegmentSize*8])
			t.Log("expected    :", expected)
			t.Log("expected len:", len(expected))
			t.Log("actual      :", actual)
			t.Log("actual   len:", len(actual))
			t.Fatal("unexpected")
		}
	}

	// Test some cases manually.
	run(0, crypto.SegmentSize)
	run(crypto.SegmentSize, crypto.SegmentSize)
	run(2*crypto.SegmentSize, crypto.SegmentSize)
	run(crypto.SegmentSize, 2*crypto.SegmentSize)
	run(1, crypto.SegmentSize)
	run(0, crypto.SegmentSize-1)
	run(0, crypto.SegmentSize+1)
	run(crypto.SegmentSize-1, crypto.SegmentSize+1)

	// Test random inputs.
	for rounds := 0; rounds < 100; rounds++ {
		// random length and offset
		length := (fastrand.Uint64n(5*crypto.SegmentSize) + 1)
		offset := fastrand.Uint64n(modules.SectorSize - length)
		run(offset, length)
	}
}

// TestLaunchedWorkerInfo_String is a small unit test that verifies the output
// of the String implementation on the launched worker info object.
func TestLaunchedWorkerInfo_String(t *testing.T) {
	t.Parallel()

	pdc := new(projectDownloadChunk)
	fastrand.Read(pdc.uid[:])

	w := new(worker)
	w.staticHostPubKey = types.SiaPublicKey{
		Algorithm: types.SignatureEd25519,
		Key:       fastrand.Bytes(32),
	}

	lwi := &launchedWorkerInfo{
		staticPieceIndex:        1,
		staticIsOverdriveWorker: false,

		staticLaunchTime:           time.Now().Add(-5 * time.Second),
		staticExpectedCompleteTime: time.Now().Add(10 * time.Second),
		staticExpectedDuration:     10 * time.Second,

		staticPDC:    pdc,
		staticWorker: w,
	}

	// assert output when download not complete
	expectedWorkerInfo := "initial worker " + w.staticHostPubKey.ShortString()
	expectedPieceInfo := "piece 1"
	expectedEstInfo := "estimated complete 10000 ms"
	expectedDurInfo := "not responded after 5000ms"
	if !strings.Contains(lwi.String(), expectedWorkerInfo) ||
		!strings.Contains(lwi.String(), expectedPieceInfo) ||
		!strings.Contains(lwi.String(), expectedEstInfo) ||
		!strings.Contains(lwi.String(), expectedDurInfo) {
		t.Fatal("unexpected: ", lwi.String())
	}

	// assert output when download complete
	lwi.completeTime = time.Now()
	lwi.jobDuration = 20 * time.Second
	lwi.totalDuration = time.Since(lwi.staticLaunchTime)

	expectedDurInfo = "responded after 5000ms"
	expectedJobInfo := "read job took 20000ms"
	expectedErrInfo := "job completed successfully"
	if !strings.Contains(lwi.String(), expectedWorkerInfo) ||
		!strings.Contains(lwi.String(), expectedDurInfo) ||
		!strings.Contains(lwi.String(), expectedJobInfo) ||
		!strings.Contains(lwi.String(), expectedErrInfo) {
		t.Fatal("unexpected", lwi.String())
	}

	// assert output when job errored out
	lwi.jobErr = errors.New("some failure")

	expectedErrInfo = "job failed with err: some failure"
	if !strings.Contains(lwi.String(), expectedWorkerInfo) ||
		!strings.Contains(lwi.String(), expectedDurInfo) ||
		!strings.Contains(lwi.String(), expectedJobInfo) ||
		!strings.Contains(lwi.String(), expectedErrInfo) {
		t.Fatal("unexpected", lwi.String())
	}

	// assert output when worker is overdrive worker
	lwi.staticIsOverdriveWorker = true
	expectedWorkerInfo = "overdrive worker " + w.staticHostPubKey.ShortString()
	if !strings.Contains(lwi.String(), expectedWorkerInfo) {
		t.Fatal("unexpected", lwi.String())
	}
}

// newTestProjectDownloadChunk returns a PDC used for testing
func newTestProjectDownloadChunk(pcws *projectChunkWorkerSet, responseChan chan *downloadResponse) *projectDownloadChunk {
	ec := pcws.staticErasureCoder

	pieceIndices := make([]uint64, ec.NumPieces())
	for i := 0; i < len(pieceIndices); i++ {
		pieceIndices[i] = uint64(i)
	}

	if responseChan == nil {
		responseChan = make(chan *downloadResponse, 1)
	}

	return &projectDownloadChunk{
		piecesInfo: make([]pieceInfo, ec.NumPieces()),
		piecesData: make([][]byte, ec.NumPieces()),

		workerProgressMap: make(map[string]workerProgress),

		downloadResponseChan: responseChan,
		workerSet:            pcws,
		workerState:          pcws.managedWorkerState(),

		ctx: context.Background(),

		staticLaunchTime:   time.Now(),
		staticPieceIndices: pieceIndices,
	}
}

// mockWorker is a helper function that returns a worker with a pricetable
// and an initialised read queue that returns a non zero value for read
// estimates depending on the given jobTime value.
func mockWorker(jobTime time.Duration) *worker {
	spk := types.SiaPublicKey{
		Algorithm: types.SignatureEd25519,
		Key:       fastrand.Bytes(crypto.PublicKeySize),
	}

	worker := new(worker)
	worker.staticHostPubKey = spk
	worker.staticHostPubKeyStr = spk.String()

	worker.newMaintenanceState()

	// init price table
	worker.newPriceTable()
	worker.staticPriceTable().staticPriceTable = newDefaultPriceTable()
	worker.staticPriceTable().staticUpdateTime = time.Now()
	worker.staticPriceTable().staticExpiryTime = time.Now().Add(5 * time.Minute)

	// init worker cache
	wc := new(workerCache)
	atomic.StorePointer(&worker.atomicCache, unsafe.Pointer(wc))

	jrs := NewJobReadStats()
	jrs.weightedJobTime64k = float64(jobTime)

	// init queues
	worker.initJobHasSectorQueue()
	worker.initJobUpdateRegistryQueue()
	worker.initJobReadRegistryQueue()
	worker.initJobReadQueue(jrs)
	worker.initJobLowPrioReadQueue(jrs)

	return worker
}

// mockErasureCoder implements the erasure coder interface, but is an invalid
// erasure coder that returns a 0 segmentsize. It is used to test the critical
// that is thrown when an invalid EC is passed to 'getPieceOffsetAndLen'
type mockErasureCoder struct{}

func (mec *mockErasureCoder) NumPieces() int                       { return 10 }
func (mec *mockErasureCoder) MinPieces() int                       { return 1 }
func (mec *mockErasureCoder) Encode(data []byte) ([][]byte, error) { return nil, nil }
func (mec *mockErasureCoder) Identifier() skymodules.ErasureCoderIdentifier {
	return skymodules.ErasureCoderIdentifier("mock")
}
func (mec *mockErasureCoder) EncodeShards(data [][]byte) ([][]byte, error)         { return nil, nil }
func (mec *mockErasureCoder) Reconstruct(pieces [][]byte) error                    { return nil }
func (mec *mockErasureCoder) Recover(pieces [][]byte, n uint64, w io.Writer) error { return nil }
func (mec *mockErasureCoder) SupportsPartialEncoding() (uint64, bool)              { return 0, true }
func (mec *mockErasureCoder) Type() skymodules.ErasureCoderType {
	return skymodules.ErasureCoderType{9, 9, 9, 9}
}
