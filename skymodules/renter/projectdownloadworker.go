package renter

import (
	"encoding/hex"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/opentracing/opentracing-go"
	"gitlab.com/NebulousLabs/errors"
	"gitlab.com/NebulousLabs/fastrand"
	"gitlab.com/SkynetLabs/skyd/build"
	"gitlab.com/SkynetLabs/skyd/skymodules"
	"go.sia.tech/siad/types"
)

// DOWNLOAD CODE IMPROVEMENTS:
//
// the current design of the download algorithm has some key places where
// there's still room for improvement. Below we list some of the ideas that
// could contribute to a faster and more robust algorithm.
//
// 1. optimize the amount of memory allocations by using a sync.Pool: while
// benchmarking and profiling the current download algorithm we found that the
// downloads are usually bottlencecked by cpu, mostly coming from
// runtime.scanobject which indicates the GC is triggered too often. By looking
// at the memory profile, both using -inuse_objects and -alloc_objects, we can
// see that `managedHandleResponse`, `managedExecuteProgram`,
// `managedHasSector`,... all allocate a bunch of memory. In a lot of these
// areas we can use a sync.Pool with preallocated memory that gets recycled,
// avoiding needless reallocation of new memory.

// 2. add a fixed cost to account for our own bandwidth expenses: because the
// host network on Sia possibly has a lot of very cheap hosts, we should be
// offsetting the cost with a fixed cost to account for our own bandwidth
// expenses. E.g. if we use a fixed cost of 2$/TB, so ~100SC, then a worker that
// cost 1SC becomes 101SC and a worker that costs 100SC becomes 200SC. That
// makes it so the difference between those workers is 2x instead of the 100x it
// was before the fixed cost.
//
// 3. add a mechanism to slow down the DTs or switch to different DTs if we have
// too many jobs launched at once: the distribution tracker does not take into
// account worker load, that means they're chance values become too optimistic,
// which might hurt the performance of the algorithm when not taken into
// account.
//
// 4. play with the 50% number, and account for the cost of being unlucky: a
// worker set's total chance has to be greater than 50% in order for it be
// accepted as a viable worker set, this 50% number is essentially arbitry, and
// even at 50% there's still a 50% chance we fall on the other side of the
// fence. This should/could be taken into account.
//
// 5. fix the algorithm that chooses which worker to replace in your set: the
// algorithm that decides what worker to replace by a cheaper worker in the
// current working set is a bit naievely implemented. Figuring out what worker
// to replace can be a very complex algorithm. There's definitely room for
// improvement here.
//
// 6. fix the algorithm that constructs chimeras: chimeras are currently
// constructed for every bucket duration, however we could also rebuild
// chimeras, or partially rebuild chimeras, when we are swapping out cheaper
// workers in the working set. The currently algorithm considers the chimera
// worker and its cost as fixed, but that does not necessarily have to be the
// case. We can further improve this by swapping out series of workers for
// cheaper workers inside of the chimera itself.

const (
	// bucketIndexScanStep defines the step size with which we increment the
	// bucket index when we try and find the best set the first time around.
	// This works more or less in a binary search fashion where we try and
	// quickly approximate the bucket index, and then scan -12|+12 buckets
	// before and after the index we found.
	bucketIndexScanStep = 12

	// chimeraAvailabilityRateThreshold defines the number that must be reached
	// when composing a chimera from unresolved workers. If the sum of the
	// availability rate of each worker reaches this threshold we build a
	// chimera out of them.
	chimeraAvailabilityRateThreshold = float64(2)
)

var (
	// maxWaitUnresolvedWorkerUpdate defines the maximum amount of time we want
	// to wait for unresolved workers to become resolved before trying to
	// recreate the worker set.
	//
	// maxWaitUpdateWorkers defines the maximum amount of time we want to wait
	// for workers to be updated.
	//
	// NOTE: these variables are lowered in test environment currently to avoid
	// a large amount of parallel downloads. We've found that the host is
	// currently facing a locking issue causing slow reads on the CI when
	// there's a lot of parallel reads taking place. This issue is tackled by
	// the following PR https://github.com/SiaFoundation/siad/pull/50
	// (partially) and thus this build var should be removed again when that is
	// merged and rolled out fully.
	maxWaitUpdateWorkers = build.Select(build.Var{
		Standard: 25 * time.Millisecond,
		Dev:      25 * time.Millisecond,
		Testing:  250 * time.Millisecond,
	}).(time.Duration)
	maxWaitUnresolvedWorkerUpdate = build.Select(build.Var{
		Standard: 50 * time.Millisecond,
		Dev:      50 * time.Millisecond,
		Testing:  250 * time.Millisecond,
	}).(time.Duration)
)

// NOTE: all of the following defined types are used by the PDC, which is
// inherently thread un-safe, that means that these types don't not need to be
// thread safe either. If fields are marked `static` it is meant to signal they
// won't change after being set.
type (
	// downloadWorker is an interface implemented by both the individual and
	// chimera workers that represents a worker that can be used for downloads.
	downloadWorker interface {
		// completeChanceCached returns the chance this download worker
		// completes a read within a certain duration. This value is cached and
		// gets recomputed for every bucket index (which represents a duration).
		// The value returned here is recomputed by 'recalculateCompleteChance'.
		completeChanceCached() float64

		// cost returns the expected job cost for downloading a piece. If the
		// worker has already been launched, its cost will be zero.
		cost() float64

		// getPieceForDownload returns the piece to download next
		getPieceForDownload() uint64

		// identifier returns a unique identifier for the download worker, this
		// identifier can be used as a key when mapping the download worker
		identifier() string

		// markPieceForDownload allows specifying what piece to download for
		// this worker in the case the worker resolved multiple pieces
		markPieceForDownload(pieceIndex uint64)

		// pieces returns all piece indices this worker can resolve
		pieces(pdc *projectDownloadChunk) []uint64

		// worker returns the underlying worker
		worker() *worker
	}

	// chimeraWorker is a worker that's built from unresolved workers until the
	// chance it has a piece is at least 'chimeraAvailabilityRateThreshold'. At
	// that point we can treat the chimera worker the same as a resolved worker
	// in the download algorithm that constructs the best worker set.
	chimeraWorker struct {
		// staticChanceComplete is the chance this worker completes after the
		// duration at which this chimera worker was built.
		staticChanceComplete float64

		// staticCost returns the cost of the chimera worker, which is the
		// average cost taken across all workers this chimera worker is
		// comprised of, it is static because it never gets updated after the
		// chimera is finalized and this field is calculated
		staticCost float64

		// staticIdentifier uniquely identifies the chimera worker
		staticIdentifier string
	}

	// individualWorker represents a single worker object, both resolved and
	// unresolved workers in the pdc can be represented by an individual worker.
	// An individual worker can be used to build a chimera worker with. For
	// every useful worker in the workerpool, an individual worker is created,
	// this worker is update as the download progresses with information from
	// the PCWS (resolved status and pieces).
	individualWorker struct {
		// the following fields are cached and recalculated at exact times
		// within the download code algorithm
		//
		// cachedCompleteChance is the chance the worker completes after the
		// current duration with which this value was recalculated
		//
		// cachedLookupIndex is the index corresponding to the estimated
		// duration of the lookup DT.
		//
		// cachedReadDTChances is the cached chances value of the read DT
		//
		// cachedReadDTChancesInitialized is used to prevent needless
		// recalculating the read DT chances, if a worker is resolved but not
		// launched, its read DT chances do not change as they don't shift.
		cachedCompleteChance           float64
		cachedLookupIndex              int
		cachedReadDTChances            skymodules.Chances
		cachedReadDTChancesInitialized bool

		// the following fields are continuously updated on the worker, all
		// individual workers are not resolved initially, when a worker resolves
		// the piece indices are updated and the resolved status is adjusted
		pieceIndices []uint64
		resolved     bool

		// onCoolDown is a flag that indicates whether this worker's HS or RS
		// queues are on cooldown, a worker with a queue on cooldown is not
		// necessarily discounted as not useful for downloading, instead it's
		// marked as on cooldown and only used if it comes off of cooldown
		onCoolDown bool

		// currentPiece is the piece that was marked by the download algorithm
		// as the piece to download next, this is used to ensure that workers
		// in the worker are not selected for duplicate piece indices.
		currentPiece           uint64
		currentPieceLaunchedAt time.Time

		// static fields on the individual worker
		staticAvailabilityRate   float64
		staticCost               float64
		staticDownloadLaunchTime time.Time
		staticIdentifier         string
		staticLookupDistribution *skymodules.Distribution
		staticReadDistribution   *skymodules.Distribution
		staticWorker             *worker
	}

	// workerSet is a collection of workers that may or may not have been
	// launched yet in order to fulfil a download.
	workerSet struct {
		workers []downloadWorker

		staticBucketDuration time.Duration
		staticBucketIndex    int
		staticMinPieces      int
		staticNumOverdrive   int

		staticPDC *projectDownloadChunk
	}

	// coinflips is a collection of chances where every item is the chance the
	// coin will turn up heads. We use the concept of a coin because it allows
	// to more easily reason about chance calculations.
	coinflips []float64
)

// NewChimeraWorker returns a new chimera worker object.
func NewChimeraWorker(workers []*individualWorker) *chimeraWorker {
	// calculate the average cost and average (weighted) complete chance
	var totalCompleteChance float64
	var totalCost float64
	for _, w := range workers {
		// sanity check the worker is unresolved
		if w.isResolved() {
			build.Critical("developer error, a chimera is built using unresolved workers only")
		}

		totalCompleteChance += w.cachedCompleteChance * w.staticAvailabilityRate
		totalCost += w.staticCost
	}

	totalWorkers := float64(len(workers))
	avgChance := totalCompleteChance / totalWorkers
	avgCost := totalCost / totalWorkers

	return &chimeraWorker{
		staticChanceComplete: avgChance,
		staticCost:           avgCost,
		staticIdentifier:     hex.EncodeToString(fastrand.Bytes(16)),
	}
}

// completeChanceCached returns the chance this chimera completes
func (cw *chimeraWorker) completeChanceCached() float64 {
	return cw.staticChanceComplete
}

// cost returns the cost for this chimera worker, this method can only be called
// on a chimera that is finalized
func (cw *chimeraWorker) cost() float64 {
	return cw.staticCost
}

// getPieceForDownload returns the piece to download next, for a chimera worker
// this is always 0 and should never be called, which is why we add a
// build.Critical to signal developer error.
func (cw *chimeraWorker) getPieceForDownload() uint64 {
	build.Critical("developer error, should not get called on a chimera worker")
	return 0
}

// identifier returns a unqiue identifier for this worker.
func (cw *chimeraWorker) identifier() string {
	return cw.staticIdentifier
}

// markPieceForDownload takes a piece index and marks it as the piece to
// download for this worker. In the case of a chimera worker this method is
// essentially a no-op since chimera workers are never launched
func (cw *chimeraWorker) markPieceForDownload(pieceIndex uint64) {
	// this is a no-op
}

// pieces returns the piece indices this worker can dowload, chimera workers
// return all pieces as we don't know yet what pieces they can resolve, note
// that all possible piece indices are defined on the pdc to avoid unnecessary
// slice allocations for every chimera
func (cw *chimeraWorker) pieces(pdc *projectDownloadChunk) []uint64 {
	return pdc.staticPieceIndices
}

// worker returns the worker, for chimeras this is always nil since it's a
// combination of multiple workers
func (cw *chimeraWorker) worker() *worker {
	return nil
}

// cost returns the cost for this worker, depending on whether it is launched or
// not it will return either 0, or the static cost variable.
func (iw *individualWorker) cost() float64 {
	if iw.isLaunched() {
		return 0
	}
	return iw.staticCost
}

// recalculateDistributionChances gets called when the download algorithm
// decides it has to recalculate the chances that are based on the worker's
// distributions. This function will apply the necessary shifts and recalculate
// the cached fields.
func (iw *individualWorker) recalculateDistributionChances() {
	// if the read dt chances are not initialized, initialize them first
	if !iw.cachedReadDTChancesInitialized {
		iw.cachedReadDTChances = iw.staticReadDistribution.ChancesAfter()
		iw.cachedReadDTChancesInitialized = true
	}

	// if the worker is launched, we want to shift the read dt
	if iw.isLaunched() {
		readDT := iw.staticReadDistribution.Clone()
		readDT.Shift(time.Since(iw.currentPieceLaunchedAt))
		iw.cachedReadDTChances = readDT.ChancesAfter()
	}

	// if the worker is not resolved yet, we want to always shift the lookup dt
	// and use that to recalculate the expected duration index
	if !iw.isResolved() {
		lookupDT := iw.staticLookupDistribution.Clone()
		lookupDT.Shift(time.Since(iw.staticDownloadLaunchTime))
		indexForDur := skymodules.DistributionBucketIndexForDuration
		iw.cachedLookupIndex = indexForDur(lookupDT.ExpectedDuration())
	}
}

// recalculateCompleteChance calculates the chance this worker completes at
// given index. This chance is a combination of the chance it resolves and the
// chance it completes the read by the given index. The resolve (or lookup)
// chance only plays a part for workers that have not resolved yet.
//
// This function calculates the complete chance by approximation, meaning if we
// request the complete chance at 200ms, for an unresolved worker, we will
// offset the read chance with the expected duration of the lookup DT. E.g. if
// the lookup DT's expected duration is 40ms, we return the complete chance at
// 160ms. Instead of durations though, we use the indices that correspond to the
// durations.
func (iw *individualWorker) recalculateCompleteChance(index int) {
	// if the worker is resolved, simply return the read chance at given index
	if iw.isResolved() {
		iw.cachedCompleteChance = iw.cachedReadDTChances[index]
		return
	}

	// if it's not resolved, and the index is smaller than our cached lookup
	// index, we return a complete chance of zero because it has no chance of
	// completing since it's not expected to have been resolved yet
	if index < iw.cachedLookupIndex {
		iw.cachedCompleteChance = 0
		return
	}

	// otherwise return the read chance offset by the lookup index
	iw.cachedCompleteChance = iw.cachedReadDTChances[index-iw.cachedLookupIndex]
}

// completeChanceCached returns the chance this worker will complete
func (iw *individualWorker) completeChanceCached() float64 {
	return iw.cachedCompleteChance
}

// getPieceForDownload returns the piece to download next
func (iw *individualWorker) getPieceForDownload() uint64 {
	return iw.currentPiece
}

// identifier returns a unqiue identifier for this worker, for an individual
// worker this is equal to the short string version of the worker's host pubkey.
func (iw *individualWorker) identifier() string {
	return iw.staticIdentifier
}

// isLaunched returns true when this workers has been launched.
func (iw *individualWorker) isLaunched() bool {
	return !iw.currentPieceLaunchedAt.IsZero()
}

// isOnCooldown returns whether this individual worker is on cooldown.
func (iw *individualWorker) isOnCooldown() bool {
	return iw.onCoolDown
}

// isResolved returns whether this individual worker has resolved.
func (iw *individualWorker) isResolved() bool {
	return iw.resolved
}

// markPieceForDownload takes a piece index and marks it as the piece to
// download next for this worker.
func (iw *individualWorker) markPieceForDownload(pieceIndex uint64) {
	// sanity check the given piece is a piece present in the worker's pieces
	if build.Release == "testing" {
		var found bool
		for _, availPieceIndex := range iw.pieceIndices {
			if pieceIndex == availPieceIndex {
				found = true
				break
			}
		}
		if !found {
			build.Critical(fmt.Sprintf("markPieceForDownload is marking a piece that is not present in the worker's piece indices, %v does not include %v", iw.pieceIndices, pieceIndex))
		}
	}
	iw.currentPiece = pieceIndex
}

// pieces returns the piece indices this worker can download.
func (iw *individualWorker) pieces(_ *projectDownloadChunk) []uint64 {
	return iw.pieceIndices
}

// worker returns the worker.
func (iw *individualWorker) worker() *worker {
	return iw.staticWorker
}

// clone returns a shallow copy of the worker set.
func (ws *workerSet) clone() *workerSet {
	return &workerSet{
		workers: append([]downloadWorker{}, ws.workers...),

		staticBucketDuration: ws.staticBucketDuration,
		staticBucketIndex:    ws.staticBucketIndex,
		staticMinPieces:      ws.staticMinPieces,
		staticNumOverdrive:   ws.staticNumOverdrive,

		staticPDC: ws.staticPDC,
	}
}

// cheaperSetFromCandidate returns a new worker set if the given candidate
// worker can improve the cost of the worker set. The worker that is being
// swapped by the candidate is the most expensive worker possible, which is not
// necessarily the most expensive worker in the set because we have to take into
// account the pieces the worker can download.
func (ws *workerSet) cheaperSetFromCandidate(candidate downloadWorker) *workerSet {
	// convenience variables
	pdc := ws.staticPDC

	// build two maps for fast lookups
	originalIndexMap := make(map[string]int)
	piecesToIndexMap := make(map[uint64]int)
	for i, w := range ws.workers {
		originalIndexMap[w.identifier()] = i
		if _, ok := w.(*individualWorker); ok {
			piecesToIndexMap[w.getPieceForDownload()] = i
		}
	}

	// sort the workers by cost, most expensive to cheapest
	byCostDesc := append([]downloadWorker{}, ws.workers...)
	sort.Slice(byCostDesc, func(i, j int) bool {
		wCostI := byCostDesc[i].cost()
		wCostJ := byCostDesc[j].cost()
		return wCostI > wCostJ
	})

	// range over the workers
	swapIndex := -1
LOOP:
	for _, w := range byCostDesc {
		// if the candidate is not cheaper than this worker we can stop looking
		// to build a cheaper set since the workers are sorted by cost
		if candidate.cost() >= w.cost() {
			break
		}

		// if the current worker is launched, don't swap it out
		expensiveWorkerPiece, launched, _ := pdc.workerProgress(w)
		if launched {
			continue
		}

		// if the current worker is a chimera worker, and we're cheaper, swap
		expensiveWorkerIndex := originalIndexMap[w.identifier()]
		if _, ok := w.(*chimeraWorker); ok {
			swapIndex = expensiveWorkerIndex
			break LOOP
		}

		// range over the candidate's pieces and see whether we can swap
		for _, piece := range candidate.pieces(pdc) {
			// if the candidate can download the same piece as the expensive
			// worker, swap it out because it's cheaper
			if piece == expensiveWorkerPiece {
				swapIndex = expensiveWorkerIndex
				break LOOP
			}

			// if the candidate can download a piece that is currently not being
			// downloaded by anyone else, swap it as well
			_, workerForPiece := piecesToIndexMap[piece]
			if !workerForPiece {
				swapIndex = expensiveWorkerIndex
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
	var totalCost float64
	for _, w := range ws.workers {
		totalCost += w.cost()
	}

	// calculate the cost penalty using the given price per ms and apply it to
	// the worker set's expected duration.
	totalCostCurr := types.NewCurrency64(uint64(totalCost))
	return addCostPenalty(ws.staticBucketDuration, totalCostCurr, ppms)
}

// chancesAfter is a small helper function that returns a list of every worker's
// chance it's completed after the given duration.
func (ws *workerSet) chancesAfter() coinflips {
	chances := make(coinflips, len(ws.workers))
	for i, w := range ws.workers {
		chances[i] = w.completeChanceCached()
	}
	return chances
}

// chanceGreaterThanHalf returns whether the total chance this worker set
// completes the download before the given duration is more than 50%.
//
// NOTE: this function abstracts the chance a worker resolves after the given
// duration as a coinflip to make it easier to reason about the problem given
// that the workerset consists out of one or more overdrive workers.
func (ws *workerSet) chanceGreaterThanHalf() bool {
	// convert every worker into a coinflip
	coinflips := ws.chancesAfter()

	var chance float64
	switch ws.staticNumOverdrive {
	case 0:
		// if we don't have to consider any overdrive workers, the chance it's
		// all heads is the chance that needs to be greater than half
		chance = coinflips.chanceAllHeads()
	case 1:
		// if there is 1 overdrive worker, we can essentially have one of the
		// coinflips come up as tails, as long as all the others are heads
		chance = coinflips.chanceHeadsAllowOneTails()
	case 2:
		// if there are 2 overdrive workers, we can have two of them come up as
		// tails, as long as all the others are heads
		chance = coinflips.chanceHeadsAllowTwoTails()
	default:
		// if there are a lot of overdrive workers, we use an approximation by
		// summing all coinflips to see whether we are expected to be able to
		// download min pieces within the given duration
		return coinflips.chanceSum() > float64(ws.staticMinPieces)
	}

	return chance > 0.5
}

// chanceAllHeads returns the chance all coins show heads.
func (cf coinflips) chanceAllHeads() float64 {
	if len(cf) == 0 {
		return 0
	}

	chanceAllHeads := float64(1)
	for _, chanceHead := range cf {
		chanceAllHeads *= chanceHead
	}
	return chanceAllHeads
}

// chanceHeadsAllowOneTails returns the chance at least n-1 coins show heads
// where n is the amount of coins.
func (cf coinflips) chanceHeadsAllowOneTails() float64 {
	chanceAllHeads := cf.chanceAllHeads()

	totalChance := chanceAllHeads
	for _, chanceHead := range cf {
		chanceTails := 1 - chanceHead
		totalChance += (chanceAllHeads / chanceHead * chanceTails)
	}
	return totalChance
}

// chanceHeadsAllowTwoTails returns the chance at least n-2 coins show heads
// where n is the amount of coins.
func (cf coinflips) chanceHeadsAllowTwoTails() float64 {
	chanceAllHeads := cf.chanceAllHeads()
	totalChance := cf.chanceHeadsAllowOneTails()

	for i := 0; i < len(cf)-1; i++ {
		chanceIHeads := cf[i]
		chanceITails := 1 - chanceIHeads
		chanceOnlyITails := chanceAllHeads / chanceIHeads * chanceITails
		for jj := i + 1; jj < len(cf); jj++ {
			chanceJHeads := cf[jj]
			chanceJTails := 1 - chanceJHeads
			chanceOnlyIAndJJTails := chanceOnlyITails / chanceJHeads * chanceJTails
			totalChance += chanceOnlyIAndJJTails
		}
	}
	return totalChance
}

// chanceSum returns the sum of all chances
func (cf coinflips) chanceSum() float64 {
	var sum float64
	for _, flip := range cf {
		sum += flip
	}
	return sum
}

// updateWorkers will update the given set of workers in-place, we update the
// workers instead of recreating them because we found that the process of
// creating an individualWorker involves some cpu intensive steps, like gouging.
// By updating them, rather than recreating them, we avoid doing these
// computations in every iteration of the download algorithm.
func (pdc *projectDownloadChunk) updateWorkers(workers []*individualWorker) {
	ws := pdc.workerState
	ws.mu.Lock()
	defer ws.mu.Unlock()

	// make a map of all resolved workers to their piece indices
	resolved := make(map[string][]uint64, len(workers))
	for _, rw := range ws.resolvedWorkers {
		resolved[rw.worker.staticHostPubKeyStr] = rw.pieceIndices
	}

	// loop over all workers and update the resolved status and piece indices
	for _, w := range workers {
		pieceIndices, resolved := resolved[w.staticWorker.staticHostPubKeyStr]
		if !w.isResolved() && resolved {
			w.resolved = true
			w.pieceIndices = pieceIndices
		}

		// check whether the worker is on cooldown
		hsq := w.staticWorker.staticJobHasSectorQueue
		rjq := w.staticWorker.staticJobReadQueue
		w.onCoolDown = hsq.callOnCooldown() || rjq.callOnCooldown()

		// recalculate the distributions
		w.recalculateDistributionChances()
	}
}

// workers returns both resolved and unresolved workers as a single slice of
// individual workers
func (pdc *projectDownloadChunk) workers() []*individualWorker {
	ws := pdc.workerState
	ws.mu.Lock()
	defer ws.mu.Unlock()

	var workers []*individualWorker

	// convenience variables
	ec := pdc.workerSet.staticErasureCoder
	length := pdc.pieceLength
	numPieces := ec.NumPieces()

	// add all resolved workers that are deemed good for downloading
	for _, rw := range ws.resolvedWorkers {
		if !isGoodForDownload(rw.worker, rw.pieceIndices) {
			continue
		}

		jrq := rw.worker.staticJobReadQueue
		rdt := jrq.staticStats.distributionTrackerForLength(length)
		cost, _ := jrq.callExpectedJobCost(length).Float64()
		hsq := rw.worker.staticJobHasSectorQueue
		ldt := hsq.staticDT

		workers = append(workers, &individualWorker{
			resolved:     true,
			pieceIndices: rw.pieceIndices,
			onCoolDown:   jrq.callOnCooldown() || hsq.callOnCooldown(),

			staticAvailabilityRate:   hsq.callAvailabilityRate(numPieces),
			staticCost:               cost,
			staticDownloadLaunchTime: time.Now(),
			staticIdentifier:         rw.worker.staticHostPubKey.ShortString(),
			staticLookupDistribution: ldt.Distribution(0),
			staticReadDistribution:   rdt.Distribution(0),
			staticWorker:             rw.worker,
		})
	}

	// add all unresolved workers that are deemed good for downloading
	for _, uw := range ws.unresolvedWorkers {
		// exclude workers that are not useful
		w := uw.staticWorker
		if !isGoodForDownload(w, pdc.staticPieceIndices) {
			continue
		}

		jrq := w.staticJobReadQueue
		rdt := jrq.staticStats.distributionTrackerForLength(length)
		hsq := w.staticJobHasSectorQueue
		ldt := hsq.staticDT

		cost, _ := jrq.callExpectedJobCost(length).Float64()
		workers = append(workers, &individualWorker{
			resolved:     false,
			pieceIndices: pdc.staticPieceIndices,
			onCoolDown:   jrq.callOnCooldown() || hsq.callOnCooldown(),

			staticAvailabilityRate:   hsq.callAvailabilityRate(numPieces),
			staticCost:               cost,
			staticDownloadLaunchTime: time.Now(),
			staticIdentifier:         w.staticHostPubKey.ShortString(),
			staticLookupDistribution: ldt.Distribution(0),
			staticReadDistribution:   rdt.Distribution(0),
			staticWorker:             w,
		})
	}

	return workers
}

// workerProgress returns the piece that was marked on the worker to download
// next, alongside two booleans that indicate whether it was launched and
// whether it completed.
func (pdc *projectDownloadChunk) workerProgress(w downloadWorker) (uint64, bool, bool) {
	// return defaults if the worker is a chimera worker, those are not
	// downloading by definition
	iw, ok := w.(*individualWorker)
	if !ok {
		return 0, false, false
	}

	// get the marked piece for this worker
	currentPiece := w.getPieceForDownload()

	// fetch the worker's download progress, if that does not exist, it's
	// neither launched nor completed.
	workerProgress, exists := pdc.workerProgressMap[iw.identifier()]
	if !exists {
		return currentPiece, false, false
	}

	_, launched := workerProgress.launchedPieces[currentPiece]
	_, completed := workerProgress.completedPieces[currentPiece]
	return currentPiece, launched, completed
}

// launchWorkerSet will range over the workers in the given worker set and will
// try to launch every worker that has not yet been launched and is ready to
// launch.
func (pdc *projectDownloadChunk) launchWorkerSet(ws *workerSet) {
	// convenience variables
	minPieces := pdc.workerSet.staticErasureCoder.MinPieces()

	// range over all workers in the set and launch if possible
	for _, w := range ws.workers {
		// continue if the worker is a chimera worker
		iw, ok := w.(*individualWorker)
		if !ok {
			continue
		}

		// continue if the worker is already launched
		piece, isLaunched, _ := pdc.workerProgress(w)
		if isLaunched {
			continue
		}

		// launch the worker
		isOverdrive := len(pdc.launchedWorkers) >= minPieces
		_, gotLaunched := pdc.launchWorker(iw, piece, isOverdrive)

		// log the event in case we launched a worker
		if gotLaunched {
			if span := opentracing.SpanFromContext(pdc.ctx); span != nil {
				span.LogKV(
					"aWorkerLaunched", w.identifier(),
					"piece", piece,
					"overdriveWorker", isOverdrive,
					"wsDuration", ws.staticBucketDuration,
					"wsIndex", ws.staticBucketIndex,
				)
			}
		}
	}
}

// threadedLaunchProjectDownload performs the main download loop, every
// iteration we update the pdc's available pieces, construct a new worker set
// and launch every worker that can be launched from that set. Every iteration
// we check whether the download was finished.
func (pdc *projectDownloadChunk) threadedLaunchProjectDownload() {
	// grab some variables
	ws := pdc.workerState
	ec := pdc.workerSet.staticErasureCoder

	// grab the workers from the pdc, every iteration we will update this set of
	// workers to avoid needless performing gouging checks on every iteration
	workers := pdc.workers()

	// verify we have enough workers to complete the download
	if len(workers) < ec.MinPieces() {
		pdc.fail(errors.Compose(ErrRootNotFound, errors.AddContext(errNotEnoughWorkers, fmt.Sprintf("%v < %v", len(workers), ec.MinPieces()))))
		return
	}

	// register for a worker update chan
	workerUpdateChan := ws.managedRegisterForWorkerUpdate()
	prevWorkerUpdate := time.Now()

	for {
		// update the pieces
		updated := pdc.updatePieces()

		// update the workers
		if updated || time.Since(prevWorkerUpdate) > maxWaitUpdateWorkers {
			pdc.updateWorkers(workers)
			prevWorkerUpdate = time.Now()
		}

		// create a worker set and launch it
		workerSet, err := pdc.createWorkerSet(workers)
		if err != nil {
			pdc.fail(err)
			return
		}
		if workerSet != nil {
			pdc.launchWorkerSet(workerSet)
		}

		// iterate
		select {
		case <-time.After(maxWaitUnresolvedWorkerUpdate):
			// recreate the workerset after maxwait
		case <-workerUpdateChan:
			// replace the worker update channel
			workerUpdateChan = ws.managedRegisterForWorkerUpdate()
		case jrr := <-pdc.workerResponseChan:
			pdc.handleJobReadResponse(jrr)
		case <-pdc.ctx.Done():
			pdc.fail(errors.New("download timed out"))
			return
		}

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
}

// createWorkerSet tries to create a worker set from the pdc's resolved and
// unresolved workers, the maximum amount of overdrive workers in the set is
// defined by the given 'maxOverdriveWorkers' argument.
func (pdc *projectDownloadChunk) createWorkerSet(workers []*individualWorker) (*workerSet, error) {
	// can't create a workerset without download workers
	if len(workers) == 0 {
		return nil, nil
	}

	// convenience variables
	ppms := pdc.pricePerMS
	minPieces := pdc.workerSet.staticErasureCoder.MinPieces()

	// loop state
	var bestSet *workerSet
	var numOverdrive int
	var bI int

	// start numOverdrive at 1 if the dependency is set
	if pdc.workerState.staticRenter.staticDeps.Disrupt("OverdriveDownload") {
		numOverdrive = 1
	}

	// approximate the bucket index by iterating over all bucket indices using a
	// step size greater than 1, once we've found the best set, we range over
	// bI-stepsize|bi+stepSize to find the best bucket index
OUTER:
	for ; numOverdrive <= maxOverdriveWorkers; numOverdrive++ {
		for bI = 0; bI < skymodules.DistributionTrackerTotalBuckets; bI += bucketIndexScanStep {
			// create the worker set
			bDur := skymodules.DistributionDurationForBucketIndex(bI)
			mostLikelySet, escape := pdc.createWorkerSetInner(workers, minPieces, numOverdrive, bI, bDur)
			if escape {
				break OUTER
			}
			if mostLikelySet == nil {
				continue
			}

			// perform price per ms comparison
			if bestSet == nil {
				bestSet = mostLikelySet
			} else if mostLikelySet.adjustedDuration(ppms) < bestSet.adjustedDuration(ppms) {
				bestSet = mostLikelySet
			}

			// exit early if ppms in combination with the bucket duration
			// already exceeds the adjusted cost of the current best set,
			// workers would be too slow by definition
			if bestSet != nil && bDur > bestSet.adjustedDuration(ppms) {
				break OUTER
			}
		}
	}

	// if we haven't found a set, no need to try and find the optimal index
	if bestSet == nil {
		return nil, nil
	}

	// after we've found one, range over bI-12 -> bI+12 to find the optimal
	// bucket index
	bIMin, bIMax := bucketIndexRange(bI)
	for bI = bIMin; bI < bIMax; bI++ {
		// create the worker set
		bDur := skymodules.DistributionDurationForBucketIndex(bI)
		mostLikelySet, escape := pdc.createWorkerSetInner(workers, minPieces, numOverdrive, bI, bDur)
		if escape {
			break
		}
		if mostLikelySet == nil {
			continue
		}

		// perform price per ms comparison
		if bestSet == nil {
			bestSet = mostLikelySet
		} else if mostLikelySet.adjustedDuration(ppms) < bestSet.adjustedDuration(ppms) {
			bestSet = mostLikelySet
		}

		// exit early if ppms in combination with the bucket duration
		// already exceeds the adjusted cost of the current best set,
		// workers would be too slow by definition
		if bestSet != nil && bDur > bestSet.adjustedDuration(ppms) {
			break
		}
	}

	return bestSet, nil
}

// createWorkerSetInner is the inner loop that is called by createWorkerSet, it
// tries to create a worker set from the given list of workers, taking into
// account the given amount of workers and overdrive workers, but also the given
// bucket duration. It returns a workerset, and a boolean that indicates whether
// we want to break out of the (outer) loop that surrounds this function call.
func (pdc *projectDownloadChunk) createWorkerSetInner(workers []*individualWorker, minPieces, numOverdrive, bI int, bDur time.Duration) (*workerSet, bool) {
	workersNeeded := minPieces + numOverdrive

	// recalculate the complete chance at given index
	for _, w := range workers {
		w.recalculateCompleteChance(bI)
	}

	// build the download workers
	downloadWorkers := pdc.buildDownloadWorkers(workers)

	// divide the workers in most likely and less likely
	mostLikely, lessLikely := pdc.splitMostlikelyLessLikely(downloadWorkers, workersNeeded)

	// if there aren't even likely workers, escape early
	if len(mostLikely) == 0 {
		return nil, true
	}

	// build the most likely set
	mostLikelySet := &workerSet{
		workers: mostLikely,

		staticBucketDuration: bDur,
		staticBucketIndex:    bI,
		staticNumOverdrive:   numOverdrive,
		staticMinPieces:      minPieces,

		staticPDC: pdc,
	}

	// if the chance of the most likely set does not exceed 50%, it is
	// not high enough to continue, no need to continue this iteration,
	// we need to try a slower and thus more likely bucket
	//
	// NOTE: this 50% value is arbitrary, it actually even means that in 50% of
	// all cases we fall at the other side of the fence... tweaking this value
	// and calculating how often we run a bad worker set is part of the download
	// improvements listed at the top of this file.
	if !mostLikelySet.chanceGreaterThanHalf() {
		return nil, false
	}

	// now loop the less likely workers and try and swap them with the
	// most expensive workers in the most likely set
	for _, w := range lessLikely {
		cheaperSet := mostLikelySet.cheaperSetFromCandidate(w)
		if cheaperSet == nil {
			continue
		}

		// if the cheaper set's chance of completing before the given
		// duration is not greater than half we can break because the
		// `lessLikely` workers were sorted by chance
		if !cheaperSet.chanceGreaterThanHalf() {
			break
		}

		mostLikelySet = cheaperSet
	}

	return mostLikelySet, false
}

// addCostPenalty takes a certain job time and adds a penalty to it depending on
// the jobcost and the pdc's price per MS.
func addCostPenalty(jobTime time.Duration, jobCost, pricePerMS types.Currency) time.Duration {
	// If the pricePerMS is higher or equal than the cost of the job, simply
	// return without penalty.
	if pricePerMS.Cmp(jobCost) >= 0 {
		return jobTime
	}

	// Otherwise, add a penalty
	var adjusted time.Duration
	penalty, err := jobCost.Div(pricePerMS).Uint64()

	// because we multiply the penalty with milliseconds and add the jobtime we
	// have to check for overflows quite extensively, define a max penalty which
	// we'll then compare with the job time to see whether we can safely
	// calculate the adjusted duration
	penaltyMaxCheck := math.MaxInt64 / int64(time.Millisecond)
	if err != nil || penalty > math.MaxInt64 {
		adjusted = time.Duration(math.MaxInt64)
	} else if reduced := penaltyMaxCheck - int64(penalty); int64(jobTime) > reduced {
		adjusted = time.Duration(math.MaxInt64)
	} else {
		adjusted = jobTime + (time.Duration(penalty) * time.Millisecond)
	}
	return adjusted
}

// buildChimeraWorkers turns a list of individual workers into chimera workers.
func (pdc *projectDownloadChunk) buildChimeraWorkers(unresolvedWorkers []*individualWorker) []downloadWorker {
	// sort workers by chance they complete
	sort.Slice(unresolvedWorkers, func(i, j int) bool {
		eRTI := unresolvedWorkers[i].cachedCompleteChance
		eRTJ := unresolvedWorkers[j].cachedCompleteChance
		return eRTI > eRTJ
	})

	// create an array that will hold all chimera workers
	chimeras := make([]downloadWorker, 0, len(unresolvedWorkers))

	// create some loop state
	currAvail := float64(0)
	start := 0

	// loop over the unresolved workers
	for curr := 0; curr < len(unresolvedWorkers); curr++ {
		currAvail += unresolvedWorkers[curr].staticAvailabilityRate
		if currAvail >= chimeraAvailabilityRateThreshold {
			end := curr + 1
			chimera := NewChimeraWorker(unresolvedWorkers[start:end])
			chimeras = append(chimeras, chimera)

			// reset loop state
			start = end
			currAvail = 0
		}
	}
	return chimeras
}

// buildDownloadWorkers is a helper function that takes a list of individual
// workers and turns them into download workers.
func (pdc *projectDownloadChunk) buildDownloadWorkers(workers []*individualWorker) []downloadWorker {
	// create an array of download workers
	downloadWorkers := make([]downloadWorker, 0, len(workers))

	// split the workers into resolved and unresolved workers, the resolved
	// workers can be added directly to the array of download workers
	resolvedWorkers, unresolvedWorkers := splitResolvedUnresolved(workers)
	downloadWorkers = append(downloadWorkers, resolvedWorkers...)

	// the unresolved workers are used to build chimeras with
	chimeraWorkers := pdc.buildChimeraWorkers(unresolvedWorkers)
	return append(downloadWorkers, chimeraWorkers...)
}

// splitMostlikelyLessLikely takes a list of download workers alongside a
// duration and an amount of workers that are needed for the most likely set of
// workers to compplete a download (this is not necessarily equal to 'minPieces'
// workers but also takes into account an amount of overdrive workers). This
// method will split the given workers array in a list of most likely workers,
// and a list of less likely workers.
func (pdc *projectDownloadChunk) splitMostlikelyLessLikely(workers []downloadWorker, workersNeeded int) ([]downloadWorker, []downloadWorker) {
	// calculate the initial capacity of the less likely array
	var lessLikelyCap int
	if len(workers) > workersNeeded {
		lessLikelyCap = len(workers) - workersNeeded
	}

	// prepare two slices that hold the workers which are most likely and the
	// ones that are less likely
	mostLikely := make([]downloadWorker, 0, workersNeeded)
	lessLikely := make([]downloadWorker, 0, lessLikelyCap)

	// define some state variables to ensure we select workers in a way the
	// pieces are unique and we are not using a worker twice
	numPieces := pdc.workerSet.staticErasureCoder.NumPieces()
	pieces := make(map[uint64]struct{}, numPieces)
	added := make(map[string]struct{}, len(workers))

	// add worker is a helper function that adds a worker to either the most
	// likely or less likely worker array and updates our state variables
	addWorker := func(w downloadWorker, pieceIndex uint64) {
		if len(mostLikely) < workersNeeded {
			mostLikely = append(mostLikely, w)
		} else {
			lessLikely = append(lessLikely, w)
		}

		added[w.identifier()] = struct{}{}
		pieces[pieceIndex] = struct{}{}
		w.markPieceForDownload(pieceIndex)
	}

	// sort the workers by percentage chance they complete after the current
	// bucket duration, essentially sorting them from most to least likely
	sort.Slice(workers, func(i, j int) bool {
		chanceI := workers[i].completeChanceCached()
		chanceJ := workers[j].completeChanceCached()
		return chanceI > chanceJ
	})

	// loop over the workers and try to add them
	for _, w := range workers {
		// workers that have in-progress downloads are re-added as long as we
		// don't already have a worker for the piece they are downloading
		currPiece, launched, completed := pdc.workerProgress(w)
		if launched && !completed {
			_, exists := pieces[currPiece]
			if !exists {
				addWorker(w, currPiece)
				continue
			}
		}

		// loop the worker's pieces to see whether it can download a piece for
		// which we don't have a worker yet or which we haven't downloaded yet
		for _, pieceIndex := range w.pieces(pdc) {
			if pdc.piecesInfo[pieceIndex].downloaded {
				continue
			}

			_, exists := pieces[pieceIndex]
			if exists {
				continue
			}

			addWorker(w, pieceIndex)
			break // only use a worker once
		}
	}

	// loop over the workers again to fill both the most likely and less likely
	// array with the remainder of the workers, still ensuring a worker is only
	// used once, this time we don't assert the piece indices are unique as this
	// makes it possible to overdrive on the same piece
	for _, w := range workers {
		_, added := added[w.identifier()]
		if added {
			continue
		}

		for _, pieceIndex := range w.pieces(pdc) {
			if pdc.piecesInfo[pieceIndex].downloaded {
				continue
			}

			addWorker(w, pieceIndex)
			break // only use a worker once
		}
	}

	return mostLikely, lessLikely
}

// bucketIndexRange is a small helper function that returns the bucket index
// range we want to loop over after finding the first bucket index approximation
func bucketIndexRange(bI int) (int, int) {
	var bIMin int
	if bI-bucketIndexScanStep >= 0 {
		bIMin = bI - bucketIndexScanStep
	}

	bIMax := skymodules.DistributionTrackerTotalBuckets - 1
	if bI+bucketIndexScanStep <= skymodules.DistributionTrackerTotalBuckets {
		bIMax = bI + bucketIndexScanStep
	}

	return bIMin, bIMax
}

// isGoodForDownload is a helper function that returns true if and only if the
// worker meets a certain set of criteria that make it useful for downloads.
// It's only useful if it is not on any type of cooldown, if it's async ready
// and if it's not price gouging.
func isGoodForDownload(w *worker, pieces []uint64) bool {
	// workers that can't download any pieces are ignored
	if len(pieces) == 0 {
		return false
	}

	// workers on cooldown or that are non async ready are not useful
	if w.managedOnMaintenanceCooldown() || !w.managedAsyncReady() {
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

// splitResolvedUnresolved is a helper function that splits the given workers
// into resolved and unresolved worker arrays. Note that if the worker is on a
// cooldown we exclude it from the returned workers list.
func splitResolvedUnresolved(workers []*individualWorker) ([]downloadWorker, []*individualWorker) {
	resolvedWorkers := make([]downloadWorker, 0, len(workers))
	unresolvedWorkers := make([]*individualWorker, 0, len(workers))

	for _, w := range workers {
		if w.isOnCooldown() {
			continue
		}

		if w.isResolved() {
			resolvedWorkers = append(resolvedWorkers, w)
		} else {
			unresolvedWorkers = append(unresolvedWorkers, w)
		}
	}

	return resolvedWorkers, unresolvedWorkers
}
