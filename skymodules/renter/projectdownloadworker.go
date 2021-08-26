package renter

import (
	"fmt"
	"sort"
	"time"

	"gitlab.com/NebulousLabs/errors"
	"gitlab.com/SkynetLabs/skyd/build"
	"gitlab.com/SkynetLabs/skyd/skymodules"
	"go.sia.tech/siad/types"
)

type (
	// downloadWorker is an interface implemented by both the individual and
	// chimera workers that represents a worker that can be used for downloads.
	downloadWorker interface {
		// cost returns the expected job cost for downloading a piece of data
		// with given length from the worker. If the worker has already been
		// launched, its cost will be zero.
		cost(length uint64) types.Currency

		// distribution returns the worker's read distribution, for an already
		// launched worker the distribution will have been shifted by the amount
		// of time since it was launched.
		distribution() *skymodules.Distribution

		// pieces returns all piece indices this worker can resolve
		pieces() []uint64

		// worker returns the underlying worker
		worker() *worker
	}

	// chimeraWorker is a worker that's built from unresolved workers until the
	// chance it has a piece is exactly 1. At that point we can treat a chimera
	// worker exactly the same as a resolved worker in the download algorithm
	// that constructs the best worker set.
	chimeraWorker struct {
		// cachedDistribution contains a distribution that is the weighted
		// combination of all worker distrubtions in this chimera worker, it is
		// cached meaning it will only be calculated the first time the
		// distribution is requested after the chimera worker was finalized.
		cachedDistribution *skymodules.Distribution

		// remaining keeps track of how much "chance" is remaining until the
		// chimeraworker is comprised of enough to workers to be able to resolve
		// a piece. This is a helper field that avoids calculating
		// 1-SUM(weights) over and over again
		remaining float64

		distributions []*skymodules.Distribution
		weights       []float64
		workers       []*worker
	}

	// individualWorker is a struct that represents a single worker object, both
	// resolved and unresolved workers in the pdc can be represented by an
	// individual worker. An individual worker can be used to build a chimera
	// worker with.
	//
	// NOTE: extending this struct requires an update to the `split` method.
	individualWorker struct {
		launchedAt    time.Time
		pieceIndices  []uint64
		resolveChance float64

		staticLookupDistribution *skymodules.Distribution
		staticReadDistribution   *skymodules.Distribution
		staticWorker             *worker
	}

	// workerArray
	workerArray []*individualWorker

	// workerSet is a collection of workers that may or may not have been
	// launched yet in order to fulfil a download.
	workerSet struct {
		workers []downloadWorker

		staticExpectedDuration time.Duration
		staticLength           uint64
		staticMinPieces        int
	}
)

func (w workerArray) Split() (resolved, unresolved []*individualWorker) {
	for _, iw := range w {
		if iw.resolveChance == 1 {
			resolved = append(resolved, iw)
		} else {
			unresolved = append(unresolved, iw)
		}
	}
	return
}

// NewChimeraWorker returns a new chimera worker object.
func NewChimeraWorker() *chimeraWorker {
	return &chimeraWorker{remaining: 1}
}

// addWorker adds the given worker to the chimera worker.
func (cw *chimeraWorker) addWorker(w *individualWorker) *individualWorker {
	// calculate the remaining chance this chimera worker needs to be complete
	if cw.remaining == 0 {
		return w
	}

	// the given worker's chance can be higher than the remaining chance of this
	// chimera worker, in that case we have to split the worker in a part we
	// want to add, and a remainder we'll use to build the next chimera with
	toAdd := w
	var remainder *individualWorker
	if w.resolveChance > cw.remaining {
		toAdd, remainder = w.split(cw.remaining)
	}

	// update the remaining chance
	cw.remaining -= toAdd.resolveChance

	// add the worker to the chimera
	cw.distributions = append(cw.distributions, toAdd.staticReadDistribution)
	cw.weights = append(cw.weights, toAdd.resolveChance)
	cw.workers = append(cw.workers, toAdd.staticWorker)
	return remainder
}

// cost implements the downloadWorker interface.
func (cw *chimeraWorker) cost(length uint64) types.Currency {
	numWorkers := uint64(len(cw.workers))
	if numWorkers == 0 {
		return types.ZeroCurrency
	}

	var total types.Currency
	for _, w := range cw.workers {
		total = total.Add(w.staticJobReadQueue.callExpectedJobCost(length))
	}
	return total.Div64(numWorkers)
}

// distribution implements the downloadWorker interface.
func (cw *chimeraWorker) distribution() *skymodules.Distribution {
	if cw.remaining != 0 {
		build.Critical("developer error, chimera is not complete")
		return nil
	}

	if cw.cachedDistribution == nil && len(cw.distributions) > 0 {
		halfLife := cw.distributions[0].HalfLife()
		cw.cachedDistribution = skymodules.NewDistribution(halfLife)
		for i, distribution := range cw.distributions {
			cw.cachedDistribution.MergeWith(distribution, cw.weights[i])
		}
	}
	return cw.cachedDistribution
}

// pieces implements the downloadWorker interface, chimera workers return nil
// since we don't know yet what pieces they can resolve
func (cw *chimeraWorker) pieces() []uint64 {
	return nil
}

// worker implements the downloadWorker interface, chimera workers return nil
// since it's comprised of multiple workers
func (cw *chimeraWorker) worker() *worker {
	return nil
}

// cost implements the downloadWorker interface.
func (iw *individualWorker) cost(length uint64) types.Currency {
	// workers that have already been launched have a zero cost
	if iw.isLaunched() {
		return types.ZeroCurrency
	}
	return iw.staticWorker.staticJobReadQueue.callExpectedJobCost(length)
}

// distribution implements the downloadWorker interface.
func (iw *individualWorker) distribution() *skymodules.Distribution {
	// if the worker has been launched already, we want to shift the
	// distribution with the time that elapsed since it was launched
	//
	// NOTE: we always shift on a clone of the original read distribution to
	// avoid shifting the same distribution multiple times
	if iw.isLaunched() {
		clone := iw.staticReadDistribution.Clone()
		clone.Shift(time.Since(iw.launchedAt))
		return clone
	}
	return iw.staticReadDistribution
}

// isLaunched returns true when this workers has been launched.
func (iw *individualWorker) isLaunched() bool {
	return !iw.launchedAt.IsZero()
}

// pieces implements the downloadWorker interface.
func (iw *individualWorker) pieces() []uint64 {
	return iw.pieceIndices
}

// worker implements the downloadWorker interface.
func (iw *individualWorker) worker() *worker {
	return iw.staticWorker
}

// split will split the download worker into two workers, the first worker will
// have the given chance, the second worker will have the remainder as its
// chance value.
func (iw *individualWorker) split(chance float64) (*individualWorker, *individualWorker) {
	if chance >= iw.resolveChance {
		build.Critical("chance value on which we split should be strictly less than the worker's resolve chance")
		return nil, nil
	}

	main := &individualWorker{
		launchedAt:    iw.launchedAt,
		pieceIndices:  iw.pieceIndices,
		resolveChance: chance,

		staticLookupDistribution: iw.staticLookupDistribution,
		staticReadDistribution:   iw.staticReadDistribution,
		staticWorker:             iw.staticWorker,
	}
	remainder := &individualWorker{
		launchedAt:    iw.launchedAt,
		pieceIndices:  iw.pieceIndices,
		resolveChance: iw.resolveChance - chance,

		staticLookupDistribution: iw.staticLookupDistribution,
		staticReadDistribution:   iw.staticReadDistribution,
		staticWorker:             iw.staticWorker,
	}

	return main, remainder
}

// clone returns a shallow copy of the worker set.
func (ws *workerSet) clone() *workerSet {
	return &workerSet{
		workers: append([]downloadWorker{}, ws.workers...),

		staticExpectedDuration: ws.staticExpectedDuration,
		staticLength:           ws.staticLength,
		staticMinPieces:        ws.staticMinPieces,
	}
}

// cheaperSetFromCandidate returns a new worker set if the given candidate
// worker can improve the cost of the worker set. The worker that is being
// swapped by the candidate is the most expensive worker possible, which is not
// necessarily the most expensive worker in the set because we have to take into
// account the pieces the worker can download.
func (ws *workerSet) cheaperSetFromCandidate(candidate downloadWorker) *workerSet {
	// build a map of what piece is fetched by what worker
	pieces := make(map[uint64]string)
	indices := make(map[string]int)
	for i, w := range ws.workers {
		worker := w.worker().staticHostPubKeyStr
		indices[worker] = i
		for _, piece := range w.pieces() {
			pieces[piece] = worker
		}
	}

	// sort the workers by cost, most expensive to cheapest
	byCostDesc := append([]downloadWorker{}, ws.workers...)
	sort.Slice(byCostDesc, func(i, j int) bool {
		wCostI := byCostDesc[i].cost(ws.staticLength)
		wCostJ := byCostDesc[j].cost(ws.staticLength)
		return wCostI.Cmp(wCostJ) > 0
	})

	// range over the workers
	swapIndex := -1
LOOP:
	for _, expensiveWorker := range byCostDesc {
		expensiveWorkerKey := expensiveWorker.worker().staticHostPubKeyStr

		// if the candidate is not cheaper than this worker we can stop looking
		// to build a cheaper set since the workers are sorted by cost
		if candidate.cost(ws.staticLength).Cmp(expensiveWorker.cost(ws.staticLength)) >= 0 {
			break
		}

		// the candidate is only useful if it can replace a worker and
		// contribute pieces to the worker set for which we don't already have
		// another worker
		for _, piece := range candidate.pieces() {
			existingWorkerKey, exists := pieces[piece]
			if !exists || existingWorkerKey == expensiveWorkerKey {
				swapIndex = indices[expensiveWorkerKey]
				break LOOP
			}
		}
	}

	if swapIndex > -1 {
		cheaperSet := ws.clone()
		cheaperSet.workers[swapIndex] = candidate
		return cheaperSet
	}
	return nil
}

// adjustedDuration returns the cost adjusted expected duration of the worker
// set using the given price per ms.
func (ws *workerSet) adjustedDuration(ppms types.Currency) time.Duration {
	// calculate the total cost of the worker set
	var totalCost types.Currency
	for _, w := range ws.workers {
		totalCost = totalCost.Add(w.cost(ws.staticLength))
	}

	// calculate the cost penalty using the given price per ms and apply it to
	// the worker set's expected duration.
	return addCostPenalty(ws.staticExpectedDuration, totalCost, ppms)
}

// chanceGreaterThanHalf returns whether the total chance this worker set
// completes the download before the given duration is more than 50%.
//
// NOTE: this function abstracts the chance a worker resolves after the given
// duration as a coinflip to make it easier to reason about the problem given
// that the workerset consists out of one or more overdrive workers.
func (ws workerSet) chanceGreaterThanHalf(dur time.Duration) bool {
	// convert every worker into a coinflip
	coinflips := make([]float64, len(ws.workers))
	for i, w := range ws.workers {
		coinflips[i] = w.distribution().ChanceAfter(dur)
	}

	// calculate the chance of all heads
	chanceAllHeads := float64(1)
	for _, flip := range coinflips {
		chanceAllHeads *= flip
	}

	// if we don't have to consider any overdrive workers, the chance it's all
	// heads is the chance that needs to be greater than half
	if ws.numOverdriveWorkers() == 0 {
		return chanceAllHeads > 0.5
	}

	// if there is 1 overdrive worker, meaning that we have 1 worker that can
	// come up as tails essentially, we have to consider the chance that one
	// coin is tails and all others come up as heads as well, and add this to
	// the chance that they're all heads
	if ws.numOverdriveWorkers() == 1 {
		totalChance := chanceAllHeads
		for _, flip := range coinflips {
			totalChance += (chanceAllHeads / flip * (1 - flip))
		}
		return totalChance > 0.5
	}

	// if there are 2 overdrive workers, meaning that we have 2 workers that can
	// up as tails, we have to extend the previous case with the case where
	// every possible coin pair comes up as being the only tails
	if ws.numOverdriveWorkers() == 2 {
		totalChance := chanceAllHeads

		// chance each cone is the only tails
		for _, flip := range coinflips {
			totalChance += chanceAllHeads / flip * (1 - flip)
		}

		// chance each possible coin pair becomes the only tails
		numCoins := len(coinflips)
		for i := 0; i < numCoins-1; i++ {
			chanceITails := 1 - coinflips[i]
			chanceOnlyITails := chanceAllHeads / coinflips[i] * chanceITails
			for jj := i + 1; jj < numCoins; jj++ {
				chanceJTails := 1 - coinflips[jj]
				chanceOnlyIAndJJTails := chanceOnlyITails / coinflips[jj] * chanceJTails
				totalChance += chanceOnlyIAndJJTails
			}
		}
		return totalChance > 0.5
	}

	// if there are a lot of overdrive workers, we use an approximation by
	// summing all coinflips to see whether we are expected to be able to
	// download min pieces within the given duration
	expected := float64(0)
	for _, flip := range coinflips {
		expected += flip
	}
	return expected > float64(ws.staticMinPieces)
}

// numOverdriveWorkers returns the number of overdrive workers in the worker
// set.
func (ws workerSet) numOverdriveWorkers() int {
	numWorkers := len(ws.workers)
	if numWorkers < ws.staticMinPieces {
		return 0
	}
	return numWorkers - ws.staticMinPieces
}

func (pdc *projectDownloadChunk) updateWorkers(workers workerArray) {
	ws := pdc.workerState
	ws.mu.Lock()
	defer ws.mu.Unlock()

	// create two maps to help update the workers
	resolved := make(map[string]struct{})
	unresolved := make(map[string]int)
	for i, w := range workers {
		if w.resolveChance == 1 {
			resolved[w.staticWorker.staticHostPubKeyStr] = struct{}{}
		} else {
			unresolved[w.staticWorker.staticHostPubKeyStr] = i
		}
	}

	// now iterate over all resolved workers, if we did not have that worker as
	// resolved before, it became resolved and we want to update it
	for _, rw := range ws.resolvedWorkers {
		_, known := resolved[rw.worker.staticHostPubKeyStr]
		if known {
			continue
		}

		index, exists := unresolved[rw.worker.staticHostPubKeyStr]
		if !exists {
			build.Critical("unresolved worker not found")
			continue
		}
		if cap(workers[index].pieceIndices) != cap(rw.pieceIndices) {
			fmt.Printf("CAP WRONG %v != %v\n", cap(workers[index].pieceIndices), cap(rw.pieceIndices))
		}

		fmt.Println("before", workers[index].pieceIndices)
		fmt.Println("setting", rw.pieceIndices)
		workers[index].launchedAt = pdc.launchedAt(rw.worker)
		workers[index].pieceIndices = rw.pieceIndices
		fmt.Println("after", workers[index].pieceIndices)
		workers[index].resolveChance = 1
	}
}

// workers returns the resolved and unresolved workers as separate slices
func (pdc *projectDownloadChunk) workers() (workerArray, <-chan struct{}) {
	ws := pdc.workerState
	ws.mu.Lock()
	defer ws.mu.Unlock()

	var workers workerArray

	// convenience variables
	ec := pdc.workerSet.staticErasureCoder
	length := pdc.pieceLength

	for _, rw := range ws.resolvedWorkers {
		// exclude workers that are not useful
		if !isGoodForDownload(rw.worker) {
			continue
		}

		w := rw.worker
		stats := w.staticJobReadQueue.staticStats
		rdt := stats.distributionTrackerForLength(length)
		ldt := w.staticJobHasSectorQueue.staticDT
		workers = append(workers, &individualWorker{
			launchedAt:    pdc.launchedAt(w),
			pieceIndices:  rw.pieceIndices,
			resolveChance: 1,

			staticLookupDistribution: ldt.Distribution(0),
			staticReadDistribution:   rdt.Distribution(0),
			staticWorker:             w,
		})
	}

	for _, uw := range ws.unresolvedWorkers {
		w := uw.staticWorker
		stats := w.staticJobReadQueue.staticStats
		rdt := stats.distributionTrackerForLength(length)
		ldt := w.staticJobHasSectorQueue.staticDT

		// exclude workers that are not useful
		if !isGoodForDownload(w) {
			continue
		}

		// unresolved workers can still have all pieces
		pieceIndices := make([]uint64, ec.MinPieces())
		for i := 0; i < len(pieceIndices); i++ {
			pieceIndices[i] = uint64(i)
		}

		workers = append(workers, &individualWorker{
			pieceIndices:  pieceIndices,
			resolveChance: w.staticJobHasSectorQueue.callAvailabilityRate(ec.NumPieces()),

			staticLookupDistribution: ldt.Distribution(0),
			staticReadDistribution:   rdt.Distribution(0),
			staticWorker:             w,
		})
	}

	return workers, ws.registerForWorkerUpdate()
}

// launchedAt returns the time at which this worker was launched.
func (pdc *projectDownloadChunk) launchedAt(w *worker) time.Time {
	for _, lw := range pdc.launchedWorkers {
		if lw.staticWorker.staticHostPubKeyStr == w.staticHostPubKeyStr {
			return lw.staticLaunchTime
		}
	}
	return time.Time{}
}

// workerIsLaunched returns true if the given worker is launched for the piece
// with given piece index.
func (pdc *projectDownloadChunk) workerIsLaunched(w *worker, pieceIndex uint64) bool {
	for _, lw := range pdc.launchedWorkers {
		if lw.staticWorker.staticHostPubKeyStr == w.staticHostPubKeyStr && lw.staticPieceIndex == pieceIndex {
			return true
		}
	}
	return false
}

// workerIsDownloading returns true if the given worker is launched and has not
// returned yet.
func (pdc *projectDownloadChunk) workerIsDownloading(w *worker) bool {
	for _, lw := range pdc.launchedWorkers {
		lwKey := lw.staticWorker.staticHostPubKeyStr
		if lwKey == w.staticHostPubKeyStr && lw.completeTime == (time.Time{}) {
			return true
		}
	}
	return false
}

// launchWorkerSet will try to launch every wo
func (pdc *projectDownloadChunk) launchWorkerSet(ws *workerSet) {
	// TODO: validate worker set (unique pieces)

	// convenience variables
	minPieces := pdc.workerSet.staticErasureCoder.MinPieces()

	// range over all workers in the set and launch if possible
	for _, w := range ws.workers {
		worker := w.worker()
		if worker == nil {
			continue // chimera worker
		}
		if pdc.workerIsDownloading(worker) {
			continue // in progress (this ensures we can re-use a worker)
		}

		fmt.Printf("worker %v has %v pieces to launch", worker.staticHostPubKeyStr[:46], w.pieces())
		for _, pieceIndex := range w.pieces() {
			if pdc.workerIsLaunched(worker, pieceIndex) {
				fmt.Println("worker already launched for piece", pieceIndex)
				continue
			}
			isOverdrive := len(pdc.launchedWorkers) >= minPieces
			fmt.Println("launch worker for piece", pieceIndex)
			_, launched := pdc.launchWorker(worker, pieceIndex, isOverdrive)
			if launched {
				fmt.Println("LAUNCHED", pieceIndex)
				break // only launch one piece per worker
			}
		}
	}
	return
}

// launchWorkers performs the main download loop, every iteration we update the
// pdc's available pieces, construct a new worker set and launch every worker
// that can be launched from that set. Every iteration we check whether the
// download was finished.
func (pdc *projectDownloadChunk) launchWorkers() {
	pdc.updateAvailablePieces()

	workers, workerUpdateChan := pdc.workers()
	for {
		// create a worker set and launch it
		workerSet, err := pdc.createWorkerSet(workers, maxOverdriveWorkers)
		if err != nil {
			pdc.fail(err)
			return
		}
		if workerSet != nil {
			pdc.launchWorkerSet(workerSet)
		}

		// iterate
		select {
		case <-time.After(20 * time.Millisecond):
			// recreate the workerset every 20ms
		case <-workerUpdateChan:
			// update the available pieces list
			pdc.updateAvailablePieces()
		case jrr := <-pdc.workerResponseChan:
			pdc.handleJobReadResponse(jrr)

			// check whether the download is completed
			completed, err := pdc.finished()
			if completed {
				pdc.finalize()
				return
			}
			if err != nil {
				pdc.fail(err)
				return
			}
		}

		// update the workers on every iteration
		pdc.updateWorkers(workers)
	}
}

// createWorkerSet tries to create a worker set from the pdc's resolved and
// unresolved workers, the maximum amount of overdrive workers in the set is
// defined by the given 'maxOverdriveWorkers' argument.
func (pdc *projectDownloadChunk) createWorkerSet(allWorkers workerArray, maxOverdriveWorkers int) (*workerSet, error) {
	// convenience variables
	ppms := pdc.pricePerMS
	length := pdc.pieceLength
	minPieces := pdc.workerSet.staticErasureCoder.MinPieces()

	// fetch all workers
	resolvedWorkers, unresolvedWorkers := allWorkers.Split()

	// verify we have enough workers to complete the download
	if len(resolvedWorkers)+len(unresolvedWorkers) < minPieces {
		return nil, errors.Compose(ErrRootNotFound, errors.AddContext(errNotEnoughWorkers, fmt.Sprintf("%v < %v", len(resolvedWorkers)+len(unresolvedWorkers), minPieces)))
	}

	// sort unresolved workers by expected resolve time
	sort.Slice(unresolvedWorkers, func(i, j int) bool {
		dI := unresolvedWorkers[i].staticLookupDistribution
		dJ := unresolvedWorkers[j].staticLookupDistribution
		return dI.ExpectedDuration() < dJ.ExpectedDuration()
	})

	// combine unresolved workers into a set of chimera workers
	var chimeraWorkers []*chimeraWorker
	current := NewChimeraWorker()
	for _, uw := range unresolvedWorkers {
		remainder := current.addWorker(uw)
		if remainder != nil {
			if remainder.resolveChance == 0 {
				build.Critical("it's in the remainder")
			}
			// add the current chimera worker to the slice and reset
			chimeraWorkers = append(chimeraWorkers, current)
			current = NewChimeraWorker()
			current.addWorker(remainder)
		}
	}

	// combine all workers
	workers := make([]downloadWorker, len(resolvedWorkers)+len(chimeraWorkers))
	for rwi, rw := range resolvedWorkers {
		workers[rwi] = rw
	}
	for cwi, cw := range chimeraWorkers {
		workers[len(resolvedWorkers)+cwi] = cw
	}
	if len(workers) == 0 {
		return nil, nil
	}

	// grab the first worker's distribution, we only need it to get at the
	// number of buckets and to transform bucket indices to a duration in ms.
	distribution := workers[0].distribution()

	// loop state
	var bestSet *workerSet
	var bestSetFound bool

OUTER:
	for numOverdrive := 0; numOverdrive < maxOverdriveWorkers; numOverdrive++ {
		workersNeeded := minPieces + numOverdrive
		for bI := 0; bI < distribution.NumBuckets(); bI++ {
			bDur := distribution.DurationForIndex(bI)

			// exit early if ppms in combination with the bucket duration
			// already exceeds the adjusted cost of the current best set,
			// workers would be too slow by definition
			if bestSetFound && bDur > bestSet.adjustedDuration(ppms) {
				break OUTER
			}

			// sort the workers by percentage chance they complete after the
			// current bucket duration
			sort.Slice(workers, func(i, j int) bool {
				chanceI := workers[i].distribution().ChanceAfter(bDur)
				chanceJ := workers[j].distribution().ChanceAfter(bDur)
				return chanceI > chanceJ
			})

			// group the most likely workers to complete in the current duration
			// in a way that we ensure no two workers are going after the same
			// piece
			mostLikely := make([]downloadWorker, 0)
			lessLikely := make([]downloadWorker, 0)
			pieces := make(map[uint64]struct{})
			for _, w := range workers {
				for _, pieceIndex := range w.pieces() {
					_, exists := pieces[pieceIndex]
					if exists {
						continue
					}
					pieces[pieceIndex] = struct{}{}

					if len(mostLikely) < workersNeeded {
						mostLikely = append(mostLikely, w)
					} else {
						lessLikely = append(lessLikely, w)
					}
					break // only use a worker once
				}
			}

			mostLikelySet := &workerSet{
				workers: mostLikely,

				staticExpectedDuration: bDur,
				staticLength:           length,
				staticMinPieces:        minPieces,
			}

			// if the chance of the most likely set does not exceed 50%, it is
			// not high enough to continue, no need to continue this iteration,
			// we need to try a slower and thus more likely bucket
			if !mostLikelySet.chanceGreaterThanHalf(bDur) {
				continue
			}

			// now loop the remaining workers and try and swap them with the
			// most expensive workers in the most likely set
			for _, w := range lessLikely {
				cheaperSet := mostLikelySet.cheaperSetFromCandidate(w)
				if cheaperSet == nil {
					continue
				}
				if !cheaperSet.chanceGreaterThanHalf(bDur) {
					break
				}
				mostLikelySet = cheaperSet
			}

			// perform price per ms comparison
			if !bestSetFound {
				bestSet = mostLikelySet
				bestSetFound = true
			} else {
				if mostLikelySet.adjustedDuration(ppms) < bestSet.adjustedDuration(ppms) {
					bestSet = mostLikelySet
				}
			}
		}
	}

	return bestSet, nil
}

// isGoodForDownload is a helper function that returns true if and only if the
// worker meets a certain set of criteria that make it useful for downloads.
// It's only useful if it is not on any type of cooldown, if it's async ready
// and if it's not price gouging.
func isGoodForDownload(w *worker) bool {
	// workers on cooldown or that are non async ready are not useful
	if w.managedOnMaintenanceCooldown() || !w.managedAsyncReady() {
		return false
	}

	// workers with a read queue on cooldown
	hsq := w.staticJobHasSectorQueue
	rjq := w.staticJobReadQueue
	if hsq.onCooldown() || rjq.onCooldown() {
		return false
	}

	// workers that are price gouging are not useful
	pt := w.staticPriceTable().staticPriceTable
	allowance := w.staticCache().staticRenterAllowance
	if err := checkProjectDownloadGouging(pt, allowance); err != nil {
		return false
	}

	return true
}
