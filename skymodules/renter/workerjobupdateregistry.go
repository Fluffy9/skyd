package renter

import (
	"context"
	"strings"
	"time"

	"github.com/opentracing/opentracing-go"
	"gitlab.com/NebulousLabs/Sia/modules"
	"gitlab.com/NebulousLabs/Sia/modules/host/registry"
	"gitlab.com/NebulousLabs/Sia/types"
	"gitlab.com/SkynetLabs/skyd/build"

	"gitlab.com/NebulousLabs/errors"
)

const (
	// jobUpdateRegistryPerformanceDecay defines how much the average
	// performance is decayed each time a new datapoint is added. The jobs use
	// an exponential weighted average.
	jobUpdateRegistryPerformanceDecay = 0.9
)

// errHostOutdatedProof is returned if the host provides a proof that has a
// valid signature but is still invalid due to its revision number.
var errHostOutdatedProof = errors.New("host returned proof with invalid revision number")

// errHostLowerRevisionThanCache is returned whenever the host claims that the
// latest revision of the registry entry it knows is lower than the one it is
// supposed to have according to the cache.
var errHostLowerRevisionThanCache = errors.New("host claims that the latest revision it knows is lower than the one in the cache")

type (
	// jobUpdateRegistry contains information about a UpdateRegistry query.
	jobUpdateRegistry struct {
		staticSiaPublicKey        types.SiaPublicKey
		staticSignedRegistryValue modules.SignedRegistryValue
		staticSpan                opentracing.Span

		staticResponseChan chan *jobUpdateRegistryResponse // Channel to send a response down

		*jobGeneric
	}

	// jobUpdateRegistryQueue is a list of UpdateRegistry jobs that have been
	// assigned to the worker.
	jobUpdateRegistryQueue struct {
		// These variables contain an exponential weighted average of the
		// worker's recent performance for jobUpdateRegistryQueue.
		weightedJobTime float64

		*jobGenericQueue
	}

	// jobUpdateRegistryResponse contains the result of a UpdateRegistry query.
	jobUpdateRegistryResponse struct {
		srv       *modules.SignedRegistryValue // only sent on ErrLowerRevNum and ErrSameRevNum
		staticErr error
	}
)

// newJobUpdateRegistry is a helper method to create a new UpdateRegistry job.
func (w *worker) newJobUpdateRegistry(ctx context.Context, span opentracing.Span, responseChan chan *jobUpdateRegistryResponse, spk types.SiaPublicKey, srv modules.SignedRegistryValue) *jobUpdateRegistry {
	jobSpan := opentracing.StartSpan("UpdateRegistryJob", opentracing.ChildOf(span.Context()))
	jobSpan.SetTag("Host", w.staticHostPubKeyStr)
	return &jobUpdateRegistry{
		staticSiaPublicKey:        spk,
		staticSignedRegistryValue: srv,
		staticResponseChan:        responseChan,
		staticSpan:                jobSpan,
		jobGeneric:                newJobGeneric(ctx, w.staticJobUpdateRegistryQueue, nil),
	}
}

// callDiscard will discard a job, sending the provided error.
func (j *jobUpdateRegistry) callDiscard(err error) {
	// Log info and finish span.
	j.staticSpan.LogKV("callDiscard", err)
	j.staticSpan.SetTag("success", false)
	defer j.staticSpan.Finish()

	w := j.staticQueue.staticWorker()
	errLaunch := w.staticRenter.tg.Launch(func() {
		response := &jobUpdateRegistryResponse{
			srv:       nil,
			staticErr: errors.Extend(err, ErrJobDiscarded),
		}
		select {
		case j.staticResponseChan <- response:
		case <-j.staticCtx.Done():
		case <-w.staticRenter.tg.StopChan():
		}
	})
	if errLaunch != nil {
		w.staticRenter.staticLog.Debugln("callDiscard: launch failed", err)
	}
}

// callExecute will run the UpdateRegistry job.
func (j *jobUpdateRegistry) callExecute() {
	start := time.Now()
	w := j.staticQueue.staticWorker()
	sid := modules.DeriveRegistryEntryID(j.staticSiaPublicKey, j.staticSignedRegistryValue.Tweak)

	// Finish job span at the end.
	defer j.staticSpan.Finish()

	// Capture callExecute in new span.
	span := opentracing.GlobalTracer().StartSpan("callExecute", opentracing.ChildOf(j.staticSpan.Context()))
	defer span.Finish()

	// Prepare a method to send a response asynchronously.
	sendResponse := func(srv *modules.SignedRegistryValue, err error) {
		errLaunch := w.staticRenter.tg.Launch(func() {
			response := &jobUpdateRegistryResponse{
				srv:       srv,
				staticErr: err,
			}
			select {
			case j.staticResponseChan <- response:
			case <-j.staticCtx.Done():
			case <-w.staticRenter.tg.StopChan():
			}
		})
		if errLaunch != nil {
			w.staticRenter.staticLog.Debugln("callExececute: launch failed", err)
		}
	}

	// update the rv. We ignore ErrSameRevNum and ErrLowerRevNum to not put the
	// host on a cooldown for something that's not necessarily its fault. We
	// might want to add another argument to the job that disables this behavior
	// in the future in case we are certain that a host can't contain those
	// errors.
	rv, err := j.managedUpdateRegistry()
	if errors.Contains(err, registry.ErrLowerRevNum) || errors.Contains(err, registry.ErrSameRevNum) {
		// Report the failure if the host can't provide a signed registry entry
		// with the error.
		if err := rv.Verify(j.staticSiaPublicKey.ToPublicKey()); err != nil {
			sendResponse(nil, err)
			j.staticQueue.callReportFailure(err)
			span.LogKV("error", err)
			j.staticSpan.SetTag("success", false)
			return
		}
		// If the entry is valid, check if the revision number is actually
		// invalid or if the revision numbers match but the PoW is too low.
		if j.staticSignedRegistryValue.Revision > rv.Revision ||
			(j.staticSignedRegistryValue.Revision == rv.Revision && j.staticSignedRegistryValue.HasMoreWork(rv.RegistryValue)) {
			sendResponse(nil, errHostOutdatedProof)
			j.staticQueue.callReportFailure(errHostOutdatedProof)
			span.LogKV("error", errHostOutdatedProof)
			j.staticSpan.SetTag("success", false)
			return
		}
		// If the entry is valid and the revision is also valid, check if we
		// have a higher revision number in the cache than the provided one.
		// TODO: update the cache to store the hash in addition to the revision
		// number for verifying the pow.
		cachedRevision, cached := w.staticRegistryCache.Get(sid)
		if cached && cachedRevision > rv.Revision {
			sendResponse(nil, errHostLowerRevisionThanCache)
			j.staticQueue.callReportFailure(errHostLowerRevisionThanCache)
			span.LogKV("error", errHostLowerRevisionThanCache)
			j.staticSpan.SetTag("success", false)
			w.staticRegistryCache.Set(sid, rv, true) // adjust the cache
			return
		}
		sendResponse(&rv, err)
		return
	} else if err != nil {
		sendResponse(nil, err)
		j.staticQueue.callReportFailure(err)
		span.LogKV("error", err)
		j.staticSpan.SetTag("success", false)
		return
	}

	// Success. We either confirmed the latest revision or updated the host
	// successfully.
	jobTime := time.Since(start)
	j.staticSpan.SetTag("success", true)

	// Update the registry cache.
	w.staticRegistryCache.Set(sid, j.staticSignedRegistryValue, false)

	// Send the response and report success.
	sendResponse(nil, nil)
	j.staticQueue.callReportSuccess()

	// Update the performance stats on the queue.
	jq := j.staticQueue.(*jobUpdateRegistryQueue)
	jq.mu.Lock()
	jq.weightedJobTime = expMovingAvgHotStart(jq.weightedJobTime, float64(jobTime), jobUpdateRegistryPerformanceDecay)
	jq.mu.Unlock()
}

// callExpectedBandwidth returns the bandwidth that is expected to be consumed
// by the job.
func (j *jobUpdateRegistry) callExpectedBandwidth() (ul, dl uint64) {
	return updateRegistryJobExpectedBandwidth()
}

// managedUpdateRegistry updates a registry entry on a host. If the error is
// ErrLowerRevNum or ErrSameRevNum, a signed registry value should be returned
// as proof.
func (j *jobUpdateRegistry) managedUpdateRegistry() (modules.SignedRegistryValue, error) {
	w := j.staticQueue.staticWorker()
	// Create the program.
	pt := w.staticPriceTable().staticPriceTable
	pb := modules.NewProgramBuilder(&pt, 0) // 0 duration since UpdateRegistry doesn't depend on it.
	if build.VersionCmp(w.staticCache().staticHostVersion, "1.5.5") < 0 {
		pb.V154AddUpdateRegistryInstruction(j.staticSiaPublicKey, j.staticSignedRegistryValue)
	} else {
		pb.AddUpdateRegistryInstruction(j.staticSiaPublicKey, j.staticSignedRegistryValue)
	}
	program, programData := pb.Program()
	cost, _, _ := pb.Cost(true)

	// take into account bandwidth costs
	ulBandwidth, dlBandwidth := j.callExpectedBandwidth()
	bandwidthCost := modules.MDMBandwidthCost(pt, ulBandwidth, dlBandwidth)
	cost = cost.Add(bandwidthCost)

	// Execute the program and parse the responses.
	var responses []programResponse
	responses, _, err := w.managedExecuteProgram(program, programData, types.FileContractID{}, categoryRegistryWrite, cost)
	if err != nil {
		return modules.SignedRegistryValue{}, errors.AddContext(err, "Unable to execute program")
	}
	for _, resp := range responses {
		// If a revision related error was returned, we try to parse the
		// signed registry value from the response.
		err = resp.Error
		// Check for ErrLowerRevNum.
		if err != nil && strings.Contains(err.Error(), registry.ErrLowerRevNum.Error()) {
			err = registry.ErrLowerRevNum
		}
		if err != nil && strings.Contains(err.Error(), registry.ErrSameRevNum.Error()) {
			err = registry.ErrSameRevNum
		}
		if errors.Contains(err, registry.ErrLowerRevNum) || errors.Contains(err, registry.ErrSameRevNum) {
			// Parse the proof.
			_, _, data, revision, sig, parseErr := parseSignedRegistryValueResponse(resp.Output, false)
			rv := modules.NewSignedRegistryValue(j.staticSignedRegistryValue.Tweak, data, revision, sig)
			return rv, errors.Compose(err, parseErr)
		}
		if err != nil {
			return modules.SignedRegistryValue{}, errors.AddContext(resp.Error, "Output error")
		}
		break
	}
	if len(responses) != len(program) {
		return modules.SignedRegistryValue{}, errors.New("received invalid number of responses but no error")
	}
	return modules.SignedRegistryValue{}, nil
}

// initJobUpdateRegistryQueue will init the queue for the UpdateRegistry jobs.
func (w *worker) initJobUpdateRegistryQueue() {
	// Sanity check that there is no existing job queue.
	if w.staticJobUpdateRegistryQueue != nil {
		w.staticRenter.staticLog.Critical("incorret call on initJobUpdateRegistryQueue")
		return
	}

	w.staticJobUpdateRegistryQueue = &jobUpdateRegistryQueue{
		jobGenericQueue: newJobGenericQueue(w),
	}
}

// UpdateRegistry is a helper method to run a UpdateRegistry job on a worker.
func (w *worker) UpdateRegistry(ctx context.Context, spk types.SiaPublicKey, rv modules.SignedRegistryValue) error {
	updateRegistryRespChan := make(chan *jobUpdateRegistryResponse)
	span := opentracing.GlobalTracer().StartSpan("UpdateRegistry")
	defer span.Finish()

	jur := w.newJobUpdateRegistry(ctx, span, updateRegistryRespChan, spk, rv)

	// Add the job to the queue.
	if !w.staticJobUpdateRegistryQueue.callAdd(jur) {
		return errors.New("worker unavailable")
	}

	// Wait for the response.
	var resp *jobUpdateRegistryResponse
	select {
	case <-ctx.Done():
		return errors.New("UpdateRegistry interrupted")
	case resp = <-updateRegistryRespChan:
	}
	return resp.staticErr
}

// updateRegistryUpdateJobExpectedBandwidth is a helper function that returns
// the expected bandwidth consumption of a UpdateRegistry job. This helper
// function enables getting at the expected bandwidth without having to
// instantiate a job.
func updateRegistryJobExpectedBandwidth() (ul, dl uint64) {
	return ethernetMTU, ethernetMTU // a single frame each for upload and download
}
