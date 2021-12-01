package renter

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/opentracing/opentracing-go"
	"gitlab.com/SkynetLabs/skyd/build"
	"gitlab.com/SkynetLabs/skyd/skymodules"
	"go.sia.tech/siad/crypto"
	"go.sia.tech/siad/modules"
	"go.sia.tech/siad/types"

	"gitlab.com/NebulousLabs/errors"
)

var (
	// errNotEnoughPieces is returned when there are not enough pieces found to
	// successfully complete the download
	errNotEnoughPieces = errors.New("not enough pieces to complete download")

	// errNotEnoughWorkers is returned if the working set does not have enough
	// workers to successfully complete the download
	errNotEnoughWorkers = errors.New("not enough workers to complete download")
)

type (
	// projectDownloadChunk is a bunch of state that helps to orchestrate a
	// download from a projectChunkWorkerSet.
	//
	// NOTE: the projectDownloadChunk is only ever accessed by a single thread
	// which orchestrates the download, which means that it does not need to be
	// thread safe.
	projectDownloadChunk struct {
		// Parameters for downloading a subset of the data within the chunk.
		lengthInChunk uint64
		offsetInChunk uint64

		// Values derived from the chunk download parameters. The offset and
		// length specify the offset and length that will be sent to the host,
		// which must be segment aligned.
		pieceLength uint64
		pieceOffset uint64

		// pricePerMS is the amount of money we are willing to spend on faster
		// workers. If a certain set of workers is 100ms faster, but that
		// exceeds the pricePerMS we are willing to pay for it, we won't use
		// that faster worker set. If it is within the budget however, we will
		// favor the faster and more expensive worker set.
		pricePerMS types.Currency

		// workersConsideredIndex keeps track of what workers were already
		// considered after looking at the 'resolvedWorkers' array defined on
		// the pcws. This enables the worker selection code to realize which
		// pieces in the worker set have been resolved since the last check.
		workersConsideredIndex int

		// unresolvedWorkersRemaining is the number of unresolved workers at the
		// time the available pieces were last updated. This enables counting
		// the hopeful pieces without introducing a race condition in the
		// finished check.
		unresolvedWorkersRemaining int

		// workerProgressMap keeps track what pieces are launched, and what
		// pieces were completed for every worker that was launched
		workerProgressMap map[string]workerProgress

		// piecesInfo contains a list of piece info objects for every possible
		// piece, it keeps tracks how many workers can download the piece and
		// what pieces were downloaded successfully.
		//
		// piecesData is the buffer that is used to place data as it comes back.
		// There is one piece per chunk, and pieces can be nil. To know if the
		// download is complete, the number of non-nil pieces will be counted.
		piecesInfo         []pieceInfo
		piecesData         [][]byte
		staticSkipRecovery bool

		// staticIsLowPrio indicates the priority of this download and has an
		// effect on the read jobs scheduled by the PDC.
		staticIsLowPrio bool

		// staticLaunchTime indicates when this PDC was launched.
		staticLaunchTime time.Time

		// staticPieceIndices contains a list of all possible piece indices,
		// this list is shared by all chimera workers which are essentially
		// capable of resolving all pieces, to avoid needlessly allocating mem.
		staticPieceIndices []uint64

		// The completed data gets sent down the response chan once the full
		// download is done.
		ctx                  context.Context
		downloadResponseChan chan *downloadResponse
		workerResponseChan   chan *jobReadResponse
		workerSet            *projectChunkWorkerSet
		workerState          *pcwsWorkerState

		// Debug helpers
		uid             [8]byte
		launchedWorkers []*launchedWorkerInfo
	}

	// pieceInfo is a helper struct that contains how many workers have the
	// piece and whether the piece was successfully downloaded
	pieceInfo struct {
		// available is incremented when a worker responds it has the piece, it
		// is decremented when a worker completed the download, it's used as a
		// mechanism to count whether or not we are still hopeful a download
		// will successfully complete
		available int64

		// downloaded is set to true when a piece was downloaded successfully
		downloaded bool
	}

	// workerProgress is a helper struct that keeps track of what pieces were
	// launched and what pieces completed for a worker that was selected by the
	// download algorithm
	//
	// NOTE: a completed piece is not necessarily downloaded successfully but
	// rather indicates only if the download completed or not
	workerProgress struct {
		completedPieces completedPieces
		launchedPieces  launchedPieces
	}

	// launchedWorkerInfo tracks information about the worker that has been
	// launched. It is used solely for debugging purposes to enable tracking the
	// chain of events that occurred when a download has timed out or failed.
	launchedWorkerInfo struct {
		// completeTime indicates when the worker eventually completed the
		// download
		completeTime time.Time

		// jobDuration is the total amount of time it took to complete the job
		jobDuration time.Duration

		// jobErr will contain the error in case it failed.
		jobErr error

		// totalDuration is the total amount of time it took for the worker to
		// complete the download since it was launched, or the time it took to
		// fail.
		totalDuration time.Duration

		// staticExpectedCompleteTime is an estimate of when we expect the
		// worker to have completed the download.
		staticExpectedCompleteTime time.Time

		// staticExpectedDuration is the estimated amount of time for this
		// worker to complete the download.
		staticExpectedDuration time.Duration

		// staticLaunchTime is the time at which the worker was launched
		staticLaunchTime time.Time

		// staticIsOverdriveWorker indicates whether this worker was launched as
		// one of the initial workers, or as an overdrive worker.
		staticIsOverdriveWorker bool

		// staticPieceIndex is the index of the piece the worker is set to
		// download, this index corresponds with the index of the
		// `availablePieces` array on the PDC.
		staticPieceIndex uint64

		staticPDC    *projectDownloadChunk
		staticWorker *worker
	}

	// completedPieces is a helper type that maps the piece index to a boolean
	// that indicates whether the piece download was successful
	completedPieces map[uint64]struct{}

	// launchedPieces is a helper type that maps the piece index to the time a
	// worker was launched to download the piece
	launchedPieces map[uint64]time.Time

	// downloadResponse is sent via a channel to the caller of
	// 'projectChunkWorkerSet.managedDownload'.
	downloadResponse struct {
		data []byte
		err  error

		// NOTE: externLogicalChunkData will be set after the download
		// is done and after that only the receiver of the response
		// should access it. That way, we can avoid copying the memory
		// and avoid another large allocation.
		externLogicalChunkData [][]byte

		// launchedWorkers contains a list of worker information for the workers
		// that were launched to try and complete this download. This field can
		// be used for debugging purposes should the download time out or error
		// out.
		launchedWorkers []*launchedWorkerInfo
	}
)

// String implements the String interface.
func (lwi *launchedWorkerInfo) String() string {
	pdcId := hex.EncodeToString(lwi.staticPDC.uid[:])
	hostKey := lwi.staticWorker.staticHostPubKey.ShortString()
	estimate := lwi.staticExpectedDuration.Milliseconds()

	var wDescr string
	if lwi.staticIsOverdriveWorker {
		wDescr = fmt.Sprintf("overdrive worker %v", hostKey)
	} else {
		wDescr = fmt.Sprintf("initial worker %v", hostKey)
	}

	// if download is not complete yet
	if lwi.completeTime.IsZero() {
		duration := time.Since(lwi.staticLaunchTime).Milliseconds()

		return fmt.Sprintf("%v | %v | piece %v | estimated complete %v ms | not responded after %vms", pdcId, wDescr, lwi.staticPieceIndex, estimate, duration)
	}

	// if download is complete
	var jDescr string
	if lwi.jobErr == nil {
		jDescr = "job completed successfully"
	} else {
		jDescr = fmt.Sprintf("job failed with err: %v", lwi.jobErr.Error())
	}

	totalDur := lwi.totalDuration.Milliseconds()
	jobDur := lwi.jobDuration.Milliseconds()

	return fmt.Sprintf("%v | %v | piece %v | estimated complete %v ms | responded after %vms | read job took %vms | %v", pdcId, wDescr, lwi.staticPieceIndex, estimate, totalDur, jobDur, jDescr)
}

// updatePieces updates the pieces info with availability updates from freshly
// resolved workers. Essentially, this is pulling new information from the
// overarching PCWS worker state. This function returns true if there were
// new resolved workers, and false otherwise.
func (pdc *projectDownloadChunk) updatePieces() bool {
	ws := pdc.workerState
	ws.mu.Lock()
	defer ws.mu.Unlock()

	// always update the remaining unresolved workers
	pdc.unresolvedWorkersRemaining = len(ws.unresolvedWorkers)

	// check whether an update is needed, if not return early
	if pdc.workersConsideredIndex == len(ws.resolvedWorkers) {
		return false
	}

	// add any new resolved workers to the pdc's list of available pieces.
	for i := pdc.workersConsideredIndex; i < len(ws.resolvedWorkers); i++ {
		// Increment the 'available' prop on the piece info for every piece this
		// worker can resolve.
		resp := ws.resolvedWorkers[i]
		for _, pieceIndex := range resp.pieceIndices {
			pdc.piecesInfo[pieceIndex].available++
		}

		// Log the resolved worker and its pieces
		if span := opentracing.SpanFromContext(pdc.ctx); span != nil && len(resp.pieceIndices) > 0 {
			span.LogKV(
				"aWorkerResolved", resp.worker.staticHostPubKeyStr,
				"pieces", resp.pieceIndices,
			)
		}
	}
	pdc.workersConsideredIndex = len(ws.resolvedWorkers)
	return true
}

// handleJobReadResponse will take a jobReadResponse from a worker job
// and integrate it into the set of pieces.
func (pdc *projectDownloadChunk) handleJobReadResponse(jrr *jobReadResponse) {
	// Prevent a production panic.
	if jrr == nil {
		pdc.workerSet.staticRenter.staticLog.Critical("received nil job read response in handleJobReadResponse")
		return
	}

	// Grab the metadata from the response
	downloadErr := jrr.staticErr
	metadata := jrr.staticMetadata
	worker := metadata.staticWorker
	workerKey := worker.staticHostPubKey.ShortString()
	pieceIndex := metadata.staticPieceRootIndex

	// Update the launched worker information
	launchedWorker := pdc.launchedWorkers[metadata.staticLaunchedWorkerIndex]
	launchedWorker.completeTime = time.Now()
	launchedWorker.jobDuration = jrr.staticJobTime
	launchedWorker.jobErr = jrr.staticErr
	launchedWorker.totalDuration = time.Since(launchedWorker.staticLaunchTime)

	// Update the piece information
	pdc.workerProgressMap[workerKey].completedPieces[pieceIndex] = struct{}{}
	pdc.piecesInfo[pieceIndex].available--

	// If the job failed, we can return.
	if downloadErr != nil {
		return
	}

	// If the job succeeded, mark the piece as downloaded
	pdc.piecesInfo[pieceIndex].downloaded = true

	// Decrypt the piece that has come back.
	key := pdc.workerSet.staticMasterKey.Derive(pdc.workerSet.staticChunkIndex, uint64(pieceIndex))
	_, err := key.DecryptBytesInPlace(jrr.staticData, pdc.pieceOffset/crypto.SegmentSize)
	if err != nil {
		pdc.workerSet.staticRenter.staticLog.Println("decryption of a piece failed")
		return
	}

	// The download succeeded, add the piece to the appropriate index.
	pdc.piecesData[pieceIndex] = jrr.staticData
	jrr.staticData = nil // Just in case there's a reference to the job response elsewhere.
}

// fail will send an error down the download response channel.
func (pdc *projectDownloadChunk) fail(err error) {
	// Log info and finish span.
	if span := opentracing.SpanFromContext(pdc.ctx); span != nil {
		span.LogKV("error", err)
		span.SetTag("success", false)
		span.Finish()
	}

	// Create and return a response
	dr := &downloadResponse{
		err: err,

		launchedWorkers: pdc.launchedWorkers,
	}
	pdc.downloadResponseChan <- dr
}

// recoverData recovers the data from the downloaded pieces.
func (pdc *projectDownloadChunk) recoverData() ([]byte, error) {
	// Determine the amount of bytes the EC will need to skip from the recovered
	// data when returning the data.
	skipLength := pdc.offsetInChunk % (crypto.SegmentSize * uint64(pdc.workerSet.staticErasureCoder.MinPieces()))
	recoveredBytes := uint64(pdc.lengthInChunk + skipLength)

	// Create a skipwriter that ensures we're recovering at the offset
	buf := bytes.NewBuffer(make([]byte, 0, recoveredBytes))
	skipWriter := &skipWriter{
		writer: buf,
		skip:   int(skipLength),
	}

	// Recover the pieces in to a single byte slice.
	err := pdc.workerSet.staticErasureCoder.Recover(pdc.piecesData, recoveredBytes, skipWriter)
	if err != nil {
		pdc.fail(errors.AddContext(err, "unable to complete erasure decode of download"))
	}
	return buf.Bytes(), err
}

// finalize will take the completed pieces of the download, recover them using
// the erasure coder, and then send the result down the response channel. If
// there is an error during decode, 'pdc.fail()' will be called.
func (pdc *projectDownloadChunk) finalize() {
	// Convenience Variables
	ec := pdc.workerSet.staticErasureCoder
	r := pdc.workerSet.staticRenter

	// Log info and finish span.
	if span := opentracing.SpanFromContext(pdc.ctx); span != nil {
		span.LogKV("duration", time.Since(pdc.staticLaunchTime))
		span.SetTag("success", true)
		span.Finish()
	}

	// Update the sector download statistics
	var numOverdriveWorkers uint64
	for _, lw := range pdc.launchedWorkers {
		if lw.staticIsOverdriveWorker {
			numOverdriveWorkers++
		}
	}

	// track base sector and fanout sector download separately
	if ec.MinPieces() == 1 {
		r.staticBaseSectorDownloadStats.AddDataPoint(numOverdriveWorkers)
	} else {
		r.staticFanoutSectorDownloadStats.AddDataPoint(numOverdriveWorkers)
	}

	// Recover the data if necessary.
	var data []byte
	var err error
	if !pdc.staticSkipRecovery {
		data, err = pdc.recoverData()
	}

	// Return the data to the caller.
	dr := &downloadResponse{
		data:                   data,
		externLogicalChunkData: pdc.piecesData,
		err:                    err,

		launchedWorkers: pdc.launchedWorkers,
	}
	pdc.downloadResponseChan <- dr
}

// finished returns true if the download is finished, and returns an error if
// the download is unable to complete.
func (pdc *projectDownloadChunk) finished() (bool, error) {
	// Convenience variables.
	ec := pdc.workerSet.staticErasureCoder

	// Loop over the static pieces array and count how many pieces were
	// downloaded successfully and how many pieces are still hopeful. A piece is
	// hopeful if either it was already downloaded, or if we still have
	// available workers capable of downloading it.
	downloadedPieces := 0
	hopefulPieces := 0
	for _, pieceInfo := range pdc.piecesInfo {
		if pieceInfo.downloaded {
			downloadedPieces++
			if downloadedPieces >= ec.MinPieces() {
				return true, nil
			}
		}

		if pieceInfo.downloaded || pieceInfo.available > 0 {
			hopefulPieces++
		}
	}

	// Add the number of unresolved workers to the hopeful pieces, since those
	// might still resolve and thus (optimistically) contribute to successfully
	// completing the download.
	hopefulPieces += pdc.unresolvedWorkersRemaining

	// If we don't have enough hopeful pieces, error out indicating we won't be
	// able to successfully complete the download.
	if hopefulPieces < ec.MinPieces() {
		return false, errors.Compose(ErrRootNotFound, errors.AddContext(errNotEnoughPieces, fmt.Sprintf("%v < %v", hopefulPieces, ec.MinPieces())))
	}
	return false, nil
}

// launchWorker will launch a worker and update the corresponding available
// piece.
//
// A time is returned which indicates the expected return time of the worker's
// download. A bool is returned which indicates whether or not the launch was
// successful.
func (pdc *projectDownloadChunk) launchWorker(worker *individualWorker, pieceIndex uint64, isOverdrive bool) (time.Time, bool) {
	w := worker.worker()

	// Sanity check that the pieceOffset and pieceLength are segment aligned.
	if pdc.pieceOffset%crypto.SegmentSize != 0 ||
		pdc.pieceLength%crypto.SegmentSize != 0 {
		build.Critical("pieceOffset or pieceLength is not segment aligned")
	}

	// Ensure we initialize a progress struct for this worker
	workerKey := worker.identifier()
	if _, exists := pdc.workerProgressMap[workerKey]; !exists {
		pdc.workerProgressMap[workerKey] = workerProgress{
			completedPieces: make(completedPieces),
			launchedPieces:  make(launchedPieces),
		}
	}

	// Create the read job metadata.
	launchedWorkerIndex := uint64(len(pdc.launchedWorkers))
	sectorRoot := pdc.workerSet.staticPieceRoots[pieceIndex]
	jobMetadata := jobReadMetadata{
		staticWorker:              w,
		staticSectorRoot:          sectorRoot,
		staticSpendingCategory:    categoryDownload,
		staticPieceRootIndex:      pieceIndex,
		staticLaunchedWorkerIndex: launchedWorkerIndex,
	}

	// Create the read sector job for the worker.
	jrq := w.callReadQueue(pdc.staticIsLowPrio)
	jrs := w.newJobReadSector(pdc.ctx, jrq, pdc.workerResponseChan, jobMetadata, sectorRoot, pdc.pieceOffset, pdc.pieceLength)

	// Submit the job and track the launched worker.
	expectedCompleteTime, added := jrq.callAddWithEstimate(jrs)
	if added {
		pdc.launchedWorkers = append(pdc.launchedWorkers, &launchedWorkerInfo{
			staticPieceIndex:        pieceIndex,
			staticIsOverdriveWorker: isOverdrive,

			staticLaunchTime:           time.Now(),
			staticExpectedCompleteTime: expectedCompleteTime,
			staticExpectedDuration:     time.Until(expectedCompleteTime),

			staticPDC:    pdc,
			staticWorker: w,
		})

		pdc.workerProgressMap[workerKey].launchedPieces[pieceIndex] = time.Now()
		worker.currentPieceLaunchedAt = time.Now()
	}

	return expectedCompleteTime, added
}

// getPieceOffsetAndLen is a helper function to compute the piece offset and
// length of a chunk download, given the erasure coder for the chunk, the offset
// within the chunk, and the length within the chunk.
func getPieceOffsetAndLen(ec skymodules.ErasureCoder, offset, length uint64) (pieceOffset, pieceLength uint64) {
	// Fetch the segment size of the ec.
	pieceSegmentSize, partialsSupported := ec.SupportsPartialEncoding()
	if !partialsSupported {
		// If partials are not supported, the full piece needs to be downloaded.
		pieceSegmentSize = modules.SectorSize
	}

	// Consistency check some of the erasure coder values. If the check fails,
	// return that the whole piece must be downloaded.
	if pieceSegmentSize == 0 {
		build.Critical("pcws has a bad erasure coder")
		return 0, modules.SectorSize
	}

	// Determine the download offset within a single piece. We get this by
	// dividing the chunk offset by the number of pieces and then rounding
	// down to the nearest segment size.
	//
	// This is mathematically equivalent to rounding down the chunk size to
	// the nearest chunk segment size and then dividing by the number of
	// pieces.
	pieceOffset = offset / uint64(ec.MinPieces())
	pieceOffset = pieceOffset / pieceSegmentSize
	pieceOffset = pieceOffset * pieceSegmentSize

	// Determine the length that needs to be downloaded. This is done by
	// determining the offset that the download needs to reach, and then
	// subtracting the pieceOffset from the termination offset.
	chunkSegmentSize := pieceSegmentSize * uint64(ec.MinPieces())
	chunkTerminationOffset := offset + length
	overflow := chunkTerminationOffset % chunkSegmentSize
	if overflow != 0 {
		chunkTerminationOffset += chunkSegmentSize - overflow
	}
	pieceTerminationOffset := chunkTerminationOffset / uint64(ec.MinPieces())
	pieceLength = pieceTerminationOffset - pieceOffset
	return pieceOffset, pieceLength
}
