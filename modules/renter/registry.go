package renter

import (
	"context"
	"fmt"
	"time"

	"gitlab.com/NebulousLabs/Sia/build"
	"gitlab.com/NebulousLabs/Sia/crypto"
	"gitlab.com/NebulousLabs/Sia/modules"
	"gitlab.com/NebulousLabs/Sia/modules/host/registry"
	"gitlab.com/NebulousLabs/Sia/types"
	"gitlab.com/NebulousLabs/errors"
)

var (
	// MaxRegistryReadTimeout is the default timeout used when reading from
	// the registry.
	MaxRegistryReadTimeout = build.Select(build.Var{
		Dev:      30 * time.Second,
		Standard: 5 * time.Minute,
		Testing:  3 * time.Second,
	}).(time.Duration)

	// DefaultRegistryUpdateTimeout is the default timeout used when updating
	// the registry.
	DefaultRegistryUpdateTimeout = build.Select(build.Var{
		Dev:      30 * time.Second,
		Standard: 5 * time.Minute,
		Testing:  3 * time.Second,
	}).(time.Duration)

	// ErrRegistryEntryNotFound is returned if all workers were unable to fetch
	// the entry.
	ErrRegistryEntryNotFound = errors.New("registry entry not found")

	// ErrRegistryLookupTimeout is similar to ErrRegistryEntryNotFound but it is
	// returned instead if the lookup timed out before all workers returned.
	ErrRegistryLookupTimeout = errors.New("registry entry not found within given time")

	// ErrRegistryUpdateInsufficientRedundancy is returned if updating the
	// registry failed due to running out of workers before reaching
	// MinUpdateRegistrySuccess successful updates.
	ErrRegistryUpdateInsufficientRedundancy = errors.New("registry update failed due reach sufficient redundancy")

	// ErrRegistryUpdateNoSuccessfulUpdates is returned if not a single update
	// was successful.
	ErrRegistryUpdateNoSuccessfulUpdates = errors.New("all registry updates failed")

	// ErrRegistryUpdateTimeout is returned when updating the registry was
	// aborted before reaching MinUpdateRegistrySucesses.
	ErrRegistryUpdateTimeout = errors.New("registry update timed out before reaching the minimum amount of updated hosts")

	// MinUpdateRegistrySuccesses is the minimum amount of success responses we
	// require from UpdateRegistry to be valid.
	MinUpdateRegistrySuccesses = build.Select(build.Var{
		Dev:      3,
		Standard: 3,
		Testing:  1,
	}).(int)

	// updateRegistryMemory is the amount of registry that UpdateRegistry will
	// request from the memory manager.
	updateRegistryMemory = uint64(20 * (1 << 10)) // 20kib

	// readRegistryMemory is the amount of registry that ReadRegistry will
	// request from the memory manager.
	readRegistryMemory = uint64(20 * (1 << 10)) // 20kib

	// useHighestRevDefaultTimeout is the amount of time before ReadRegistry
	// will stop waiting for additional responses from hosts and accept the
	// response with the highest rev number. The timer starts when we get the
	// first response and doesn't reset afterwards.
	useHighestRevDefaultTimeout = 100 * time.Millisecond
)

// readResponseSet is a helper type which allows for returning a set of ongoing
// ReadRegistry responses.
type readResponseSet struct {
	c    <-chan *jobReadRegistryResponse
	left int

	readResps []*jobReadRegistryResponse
}

// newReadResponseSet creates a new set from a response chan and number of
// workers which are expected to write to that chan.
func newReadResponseSet(responseChan <-chan *jobReadRegistryResponse, numWorkers int) *readResponseSet {
	return &readResponseSet{
		c:         responseChan,
		left:      numWorkers,
		readResps: make([]*jobReadRegistryResponse, 0, numWorkers),
	}
}

// collect will collect all responses. It will block until it has received all
// of them or until the provided context is closed.
func (rrs *readResponseSet) collect(ctx context.Context) []*jobReadRegistryResponse {
	for rrs.responsesLeft() > 0 {
		resp := rrs.next(ctx)
		if resp == nil {
			return nil
		}
	}
	return rrs.readResps
}

// next returns the next available response. It will block until the response is
// received or the provided context is closed.
func (rrs *readResponseSet) next(ctx context.Context) *jobReadRegistryResponse {
	select {
	case <-ctx.Done():
		return nil
	case resp := <-rrs.c:
		rrs.readResps = append(rrs.readResps, resp)
		rrs.left--
		return resp
	}
}

// responsesLeft returns the number of responses that can still be fetched with
// Next.
func (rrs *readResponseSet) responsesLeft() int {
	return rrs.left
}

// ReadRegistry starts a registry lookup on all available workers. The
// jobs have 'timeout' amount of time to finish their jobs and return a
// response. Otherwise the response with the highest revision number will be
// used.
func (r *Renter) ReadRegistry(spk types.SiaPublicKey, tweak crypto.Hash, timeout time.Duration) (modules.SignedRegistryValue, error) {
	// Block until there is memory available, and then ensure the memory gets
	// returned.
	// Since registry entries are very small we use a fairly generous multiple.
	if !r.memoryManager.Request(readRegistryMemory, memoryPriorityHigh) {
		return modules.SignedRegistryValue{}, errors.New("renter shut down before memory could be allocated for the project")
	}
	defer r.memoryManager.Return(readRegistryMemory)

	// Create a context. If the timeout is greater than zero, have the context
	// expire when the timeout triggers.
	ctx := r.tg.StopCtx()
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(r.tg.StopCtx(), timeout)
		defer cancel()
	}

	// Start the ReadRegistry jobs.
	srv, responseSet, err := r.managedReadRegistry(ctx, spk, tweak)
	if errors.Contains(err, ErrRegistryLookupTimeout) {
		err = errors.AddContext(err, fmt.Sprintf("timed out after %vs", timeout.Seconds()))
	}
	// Spawn a goroutine to handle the responses once all of them are done.
	if responseSet != nil {
		go r.threadedHandleFinishedReadRegistryResponses(spk, tweak, responseSet)
	}
	return srv, err
}

// UpdateRegistry updates the registries on all workers with the given
// registry value.
func (r *Renter) UpdateRegistry(spk types.SiaPublicKey, srv modules.SignedRegistryValue, timeout time.Duration) error {
	// Block until there is memory available, and then ensure the memory gets
	// returned.
	// Since registry entries are very small we use a fairly generous multiple.
	if !r.memoryManager.Request(updateRegistryMemory, memoryPriorityHigh) {
		return errors.New("renter shut down before memory could be allocated for the project")
	}
	defer r.memoryManager.Return(updateRegistryMemory)

	// Create a context. If the timeout is greater than zero, have the context
	// expire when the timeout triggers.
	ctx := r.tg.StopCtx()
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(r.tg.StopCtx(), timeout)
		defer cancel()
	}

	// Start the UpdateRegistry jobs.
	err := r.managedUpdateRegistry(ctx, spk, srv)
	if errors.Contains(err, ErrRegistryUpdateTimeout) {
		err = errors.AddContext(err, fmt.Sprintf("timed out after %vs", timeout.Seconds()))
	}
	return err
}

// managedReadRegistry starts a registry lookup on all available workers. The
// jobs have 'timeout' amount of time to finish their jobs and return a
// response. Otherwise the response with the highest revision number will be
// used.
func (r *Renter) managedReadRegistry(ctx context.Context, spk types.SiaPublicKey, tweak crypto.Hash) (modules.SignedRegistryValue, *readResponseSet, error) {
	// Create a context that dies when the function ends, this will cancel all
	// of the worker jobs that get created by this function.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Get the full list of workers and create a channel to receive all of the
	// results from the workers. The channel is buffered with one slot per
	// worker, so that the workers do not have to block when returning the
	// result of the job, even if this thread is not listening.
	workers := r.staticWorkerPool.callWorkers()
	staticResponseChan := make(chan *jobReadRegistryResponse, len(workers))

	// Filter out hosts that don't support the registry.
	numRegistryWorkers := 0
	for _, worker := range workers {
		cache := worker.staticCache()
		if build.VersionCmp(cache.staticHostVersion, minRegistryVersion) < 0 {
			continue
		}

		// check for price gouging
		// TODO: use PDBR gouging for some basic protection. Should be replaced
		// as part of the gouging overhaul.
		pt := worker.staticPriceTable().staticPriceTable
		err := checkPDBRGouging(pt, cache.staticRenterAllowance)
		if err != nil {
			r.log.Debugf("price gouging detected in worker %v, err: %v\n", worker.staticHostPubKeyStr, err)
			continue
		}

		jrr := worker.newJobReadRegistry(ctx, staticResponseChan, spk, tweak)
		if !worker.staticJobReadRegistryQueue.callAdd(jrr) {
			// This will filter out any workers that are on cooldown or
			// otherwise can't participate in the project.
			continue
		}
		workers[numRegistryWorkers] = worker
		numRegistryWorkers++
	}
	workers = workers[:numRegistryWorkers]
	// If there are no workers remaining, fail early.
	if len(workers) == 0 {
		return modules.SignedRegistryValue{}, nil, errors.AddContext(modules.ErrNotEnoughWorkersInWorkerPool, "cannot perform ReadRegistry")
	}

	// Create the response set.
	responseSet := newReadResponseSet(staticResponseChan, numRegistryWorkers)

	// Prepare a context which will be overwritten by a child context with a timeout
	// when we receive the first response. useHighestRevDefaultTimeout after
	// receiving the first response, this will be closed to abort the search for
	// the highest rev number and return the highest one we have so far.
	var useHighestRevCtx context.Context

	var srv *modules.SignedRegistryValue
	responses := 0

	for responseSet.responsesLeft() > 0 {
		// Check cancel condition and block for more responses.
		var resp *jobReadRegistryResponse
		if srv != nil {
			// If we have a successful response already, we wait on the highest
			// rev ctx.
			resp = responseSet.next(useHighestRevCtx)
		} else {
			// Otherwise we don't wait on the usehighestRevCtx since we need a
			// successful response to abort.
			resp = responseSet.next(ctx)
		}
		if resp == nil {
			break // context triggered
		}

		// When we get the first response, we initialize the highest rev
		// timeout.
		if responses == 0 {
			c, cancel := context.WithTimeout(ctx, useHighestRevDefaultTimeout)
			defer cancel()
			useHighestRevCtx = c
		}

		// Increment responses.
		responses++

		// Ignore error responses and responses that returned no entry.
		if resp.staticErr != nil || resp.staticSignedRegistryValue == nil {
			continue
		}

		// Remember the response with the highest revision number. We use >=
		// here to also catch the edge case of the initial revision being 0.
		if srv == nil || resp.staticSignedRegistryValue.Revision >= srv.Revision {
			srv = resp.staticSignedRegistryValue
		}
	}

	// If we don't have a successful response and also not a response for every
	// worker, we timed out. We still return the response set since there might
	// be slow successful responses that we missed.
	if srv == nil && responses < len(workers) {
		return modules.SignedRegistryValue{}, responseSet, ErrRegistryLookupTimeout
	}

	// If we don't have a successful response but received a response from every
	// worker, we were unable to look up the entry.
	if srv == nil {
		return modules.SignedRegistryValue{}, responseSet, ErrRegistryEntryNotFound
	}
	return *srv, responseSet, nil
}

// managedUpdateRegistry updates the registries on all workers with the given
// registry value.
// NOTE: the input ctx only unblocks the call if it fails to hit the threshold
// before the timeout. It doesn't stop the update jobs. That's because we want
// to always make sure we update as many hosts as possble.
func (r *Renter) managedUpdateRegistry(ctx context.Context, spk types.SiaPublicKey, srv modules.SignedRegistryValue) error {
	// Verify the signature before updating the hosts.
	if err := srv.Verify(spk.ToPublicKey()); err != nil {
		return errors.AddContext(err, "managedUpdateRegistry: failed to verify signature of entry")
	}
	// Get the full list of workers and create a channel to receive all of the
	// results from the workers. The channel is buffered with one slot per
	// worker, so that the workers do not have to block when returning the
	// result of the job, even if this thread is not listening.
	workers := r.staticWorkerPool.callWorkers()
	staticResponseChan := make(chan *jobUpdateRegistryResponse, len(workers))

	// Filter out hosts that don't support the registry.
	numRegistryWorkers := 0
	for _, worker := range workers {
		if !worker.callLaunchUpdateRegistry(spk, srv, staticResponseChan) {
			// This will filter out any workers that are on cooldown or
			// otherwise can't participate in the project.
			continue
		}
		workers[numRegistryWorkers] = worker
		numRegistryWorkers++
	}
	workers = workers[:numRegistryWorkers]
	// If there are no workers remaining, fail early.
	if len(workers) < MinUpdateRegistrySuccesses {
		return errors.AddContext(modules.ErrNotEnoughWorkersInWorkerPool, "cannot performa UpdateRegistry")
	}

	workersLeft := len(workers)
	responses := 0
	successfulResponses := 0
	highestInvalidRevNum := uint64(0)
	invalidRevNum := false

	for successfulResponses < MinUpdateRegistrySuccesses && workersLeft+successfulResponses >= MinUpdateRegistrySuccesses {
		// Check deadline.
		var resp *jobUpdateRegistryResponse
		select {
		case <-ctx.Done():
			// Timeout reached.
			return ErrRegistryUpdateTimeout
		case resp = <-staticResponseChan:
		}

		// Decrement the number of workers.
		workersLeft--

		// Increment number of responses.
		responses++

		// Ignore error responses except for invalid revision errors.
		if resp.staticErr != nil {
			// If we receive ErrLowerRevNum or ErrSameRevNum, remember the revision number
			// that was presented as proof. In the end we return the highest one to be able
			// to determine the next revision number that is save to use.
			if (errors.Contains(resp.staticErr, registry.ErrLowerRevNum) || errors.Contains(resp.staticErr, registry.ErrSameRevNum)) &&
				resp.srv.Revision > highestInvalidRevNum {
				highestInvalidRevNum = resp.srv.Revision
				invalidRevNum = true
			}
			continue
		}

		// Increment successful responses.
		successfulResponses++
	}

	// Check for an invalid revision error and return the right error according
	// to the highest invalid revision we remembered.
	var err error
	if invalidRevNum {
		if highestInvalidRevNum == srv.Revision {
			err = registry.ErrSameRevNum
		} else {
			err = registry.ErrLowerRevNum
		}
	}

	// Check if we ran out of workers.
	if successfulResponses == 0 {
		return errors.Compose(err, ErrRegistryUpdateNoSuccessfulUpdates)
	}
	if successfulResponses < MinUpdateRegistrySuccesses {
		return errors.Compose(err, ErrRegistryUpdateInsufficientRedundancy)
	}
	return nil
}

// threadedHandleFinishedReadRegistryResponses waits for all provided read
// registry programs to finish and updates all workers from responses which
// either didn't provide the highest revision number, or didn't have the entry
// at all.
func (r *Renter) threadedHandleFinishedReadRegistryResponses(spk types.SiaPublicKey, tweak crypto.Hash, responseSet *readResponseSet) {
	if err := r.tg.Add(); err != nil {
		return
	}
	defer r.tg.Done()

	// Register the update to make sure we don't try again if a value is rapidly
	// polled before this update is done.
	mapKey := crypto.HashAll(spk, tweak)
	id := r.mu.Lock()
	_, exists := r.ongoingRegistryUpdates[mapKey]
	if !exists {
		r.ongoingRegistryUpdates[mapKey] = struct{}{}
	}
	r.mu.Unlock(id)
	if exists {
		return // ongoing update found
	}

	// Unregister the update once done.
	defer func() {
		id := r.mu.Lock()
		delete(r.ongoingRegistryUpdates, mapKey)
		r.mu.Unlock(id)
	}()

	// Collect all responses.
	resps := responseSet.collect(r.tg.StopCtx())
	if resps == nil {
		return // shutdown
	}

	// Filter out the workers that didn't fail and find the highest revision
	// response.
	var srv *modules.SignedRegistryValue
	numSuccesses := 0
	for _, resp := range resps {
		if resp.staticErr != nil {
			continue
		}
		resps[numSuccesses] = resp
		numSuccesses++

		// Check if host knew registry value.
		if resp.staticSignedRegistryValue == nil {
			continue
		}
		// Otherwise remember the highest success response.
		if srv == nil || resp.staticSignedRegistryValue.Revision > srv.Revision {
			srv = resp.staticSignedRegistryValue
		}
	}

	// If none reported a value, there is nothing we can do.
	if srv == nil {
		return
	}

	// Otherwise we update all workers, that didn't have the latest revision.
	numRegistryWorkers := 0
	staticResponseChan := make(chan *jobUpdateRegistryResponse, len(resps))
	for _, resp := range resps {
		// Ignore workers that already had the latest version.
		if resp.staticSignedRegistryValue != nil && resp.staticSignedRegistryValue.Revision == srv.Revision {
			continue
		}
		if !resp.staticWorker.callLaunchUpdateRegistry(spk, *srv, staticResponseChan) {
			// This will filter out any workers that are on cooldown or
			// otherwise can't participate in the project.
			continue
		}
		numRegistryWorkers++
	}

	// Wait for all of them to be updated.
	for i := 0; i < numRegistryWorkers; i++ {
		select {
		case <-r.tg.StopChan():
			return // shutdown
		case _ = <-staticResponseChan:
		}
		// Ignore the response for now. In the future we might want some special
		// handling here.
	}
	return
}
