package renter

// worker.go defines a worker with a work loop. Each worker is connected to a
// single host, and the work loop will listen for jobs and then perform them.
//
// The worker has a set of jobs that it is capable of performing. The standard
// functions for a job are Queue, Kill, and Perform. Queue will add a job to the
// queue of work of that type. Kill will empty the queue and close out any work
// that will not be completed. Perform will grab a job from the queue if one
// exists and complete that piece of work. See workerfetchbackups.go for a clean
// example.
//
// The worker has an ephemeral account on the host. It can use this account to
// pay for downloads and uploads. In order to ensure the account's balance does
// not run out, it maintains a balance target by refilling it when necessary.
//
// TODO: A single session should be added to the worker that gets maintained
// within the work loop. All jobs performed by the worker will use the worker's
// single session.
//
// TODO: The upload and download code needs to be moved into properly separated
// subsystems.
//
// TODO: Need to write testing around the kill functions in the worker, to clean
// up any queued jobs after a worker has been killed.

import (
	"fmt"
	"sync"
	"time"

	"gitlab.com/NebulousLabs/Sia/build"
	"gitlab.com/NebulousLabs/Sia/modules"
	"gitlab.com/NebulousLabs/Sia/types"

	"gitlab.com/NebulousLabs/errors"
)

const (
	// renterHostProtocolVersion is the version threshold at which to use the
	// new renter-host protocol
	renterHostProtocolVersion = "1.4.8"
)

var (
	// workerCacheUpdateFrequency specifies how much time must pass before the
	// worker updates its cache.
	workerCacheUpdateFrequency = build.Select(build.Var{
		Dev:      time.Second * 5,
		Standard: time.Minute,
		Testing:  time.Second,
	}).(time.Duration)
)

// A worker listens for work on a certain host.
//
// The mutex of the worker only protects the 'unprocessedChunks' and the
// 'standbyChunks' fields of the worker. The rest of the fields are only
// interacted with exclusively by the primary worker thread, and only one of
// those ever exists at a time.
//
// The workers have a concept of 'cooldown' for uploads and downloads. If a
// download or upload operation fails, the assumption is that future attempts
// are also likely to fail, because whatever condition resulted in the failure
// will still be present until some time has passed. Without any cooldowns,
// uploading and downloading with flaky hosts in the worker sets has
// substantially reduced overall performance and throughput.
type worker struct {
	// The host pub key also serves as an id for the worker, as there is only
	// one worker per host.
	staticHostPubKey     types.SiaPublicKey
	staticHostPubKeyStr  string
	staticHostMuxAddress string
	staticHostVersion    string

	// Cached value for the contract utility, updated infrequently.
	cachedContractUtility modules.ContractUtility

	// Cached blockheight, updated by the renter when consensus changes. We
	// cache it on the worker to avoid fetching it from consensus on every RPC
	// call that requires to know the current block height.
	cachedBlockHeight types.BlockHeight

	// Download variables that are not protected by a mutex, but also do not
	// need to be protected by a mutex, as they are only accessed by the master
	// thread for the worker.
	//
	// The 'owned' prefix here indicates that only the master thread for the
	// object (in this case, 'threadedWorkLoop') is allowed to access these
	// variables. Because only that thread is allowed to access the variables,
	// that thread is able to access these variables without a mutex.
	ownedDownloadConsecutiveFailures int       // How many failures in a row?
	ownedDownloadRecentFailure       time.Time // How recent was the last failure?

	// Download variables related to queuing work. They have a separate mutex to
	// minimize lock contention.
	downloadChunks     []*unfinishedDownloadChunk // Yet unprocessed work items.
	downloadMu         sync.Mutex
	downloadTerminated bool // Has downloading been terminated for this worker?

	// Job queues for the worker.
	staticFetchBackupsJobQueue   fetchBackupsJobQueue
	staticJobQueueDownloadByRoot jobQueueDownloadByRoot

	// Upload variables.
	unprocessedChunks         []*unfinishedUploadChunk // Yet unprocessed work items.
	uploadConsecutiveFailures int                      // How many times in a row uploading has failed.
	uploadRecentFailure       time.Time                // How recent was the last failure?
	uploadRecentFailureErr    error                    // What was the reason for the last failure?
	uploadTerminated          bool                     // Have we stopped uploading?

	// The staticAccount represent the renter's ephemeral account on the host.
	// It keeps track of the available balance in the account, the worker has a
	// refill mechanism that keeps the account balance filled up until the
	// staticBalanceTarget.
	staticAccount       *account
	staticBalanceTarget types.Currency

	// The staticHostPrices hold information about the price table. It has its
	// own mutex becaus we check if we need to update the price table in every
	// iteration of the worker loop.
	staticHostPrices hostPrices

	// Utilities.
	//
	// The mutex is only needed when interacting with 'downloadChunks' and
	// 'unprocessedChunks', as everything else is only accessed from the single
	// master thread.
	killChan chan struct{} // Worker will shut down if a signal is sent down this channel.
	mu       sync.Mutex
	renter   *Renter
	wakeChan chan struct{} // Worker will check queues if given a wake signal.
}

// managedBlockUntilReady will block until the worker has internet connectivity.
// 'false' will be returned if a kill signal is received or if the renter is
// shut down before internet connectivity is restored. 'true' will be returned
// if internet connectivity is successfully restored.
func (w *worker) managedBlockUntilReady() bool {
	// Check if the worker has received a kill signal, or if the renter has
	// received a stop signal.
	select {
	case <-w.renter.tg.StopChan():
		return false
	case <-w.killChan:
		return false
	default:
	}

	// Check internet connectivity. If the worker does not have internet
	// connectivity, block until connectivity is restored.
	for !w.renter.g.Online() {
		select {
		case <-w.renter.tg.StopChan():
			return false
		case <-w.killChan:
			return false
		case <-time.After(offlineCheckFrequency):
		}
	}
	return true
}

// managedUpdateCache will check how recently each of the cached values of the
// worker has been updated and update anything that is not recent enough.
//
// 'false' will be returned if the cache cannot be updated, signaling that the
// worker should exit.
func (w *worker) managedUpdateCache() bool {
	w.mu.Lock()
	defer w.mu.Unlock()

	utility, exists := w.renter.hostContractor.ContractUtility(w.staticHostPubKey)
	if !exists {
		return false
	}
	w.cachedContractUtility = utility
	return true
}

// staticKilled is a convenience function to determine if a worker has been
// killed or not.
func (w *worker) staticKilled() bool {
	select {
	case <-w.killChan:
		return true
	default:
		return false
	}
}

// staticWake needs to be called any time that a job queued.
func (w *worker) staticWake() {
	select {
	case w.wakeChan <- struct{}{}:
	default:
	}
}

// threadedWorkLoop continually checks if work has been issued to a worker. The
// work loop checks for different types of work in a specific order, forming a
// priority queue for the various types of work. It is possible for continuous
// requests for one type of work to drown out a worker's ability to perform
// other types of work.
//
// If no work is found, the worker will sleep until woken up. Because each
// iteration is stateless, it may be possible to reduce the goroutine count in
// Sia by spinning down the worker / expiring the thread when there is no work,
// and then checking if the thread exists and creating a new one if not when
// alerting / waking the worker. This will not interrupt any connections that
// the worker has because the worker object will be kept in memory via the
// worker map.
func (w *worker) threadedWorkLoop() {
	// Ensure that all queued jobs are gracefully cleaned up when the worker is
	// shut down.
	//
	// TODO: Need to write testing around these kill functions and ensure they
	// are executing correctly.
	defer w.managedKillUploading()
	defer w.managedKillDownloading()
	defer w.managedKillFetchBackupsJobs()
	defer w.managedKillJobsDownloadByRoot()

	// Fetch the cache for the worker before doing any work.
	if !w.managedUpdateCache() {
		w.renter.log.Println("Worker is being insta-killed because the cache update could not locate utility for the worker")
		return
	}
	lastCacheUpdate := time.Now()

	// Primary work loop. There are several types of jobs that the worker can
	// perform, and they are attempted with a specific priority. If any type of
	// work is attempted, the loop resets to check for higher priority work
	// again. This means that a stream of higher priority tasks can starve a
	// building set of lower priority tasks.
	//
	// 'workAttempted' indicates that there was a job to perform, and that a
	// nontrivial amount of time was spent attempting to perform the job. The
	// job may or may not have been successful, that is irrelevant.
	for {
		// There are certain conditions under which the worker should either
		// block or exit. This function will block until those conditions are
		// met, returning 'true' when the worker can proceed and 'false' if the
		// worker should exit.
		if !w.managedBlockUntilReady() {
			return
		}

		// Check if the cache needs to be updated.
		if time.Since(lastCacheUpdate) > workerCacheUpdateFrequency {
			if !w.managedUpdateCache() {
				w.renter.log.Debugln("worker is being killed because the cache could not be updated")
				return
			}
			lastCacheUpdate = time.Now()
		}

		// Check if the price table needs to be updated.
		w.managedTryUpdatePriceTable()

		// Check if the account needs to be refilled.
		w.managedTryRefillAccount()

		// Perform any job to fetch the list of backups from the host.
		workAttempted := w.managedPerformFetchBackupsJob()
		if workAttempted {
			continue
		}
		// Perform any job to fetch data by its sector root. This is given
		// priority because it is only used by viewnodes, which are service
		// operators that need to have good performance for their customers.
		workAttempted = w.managedLaunchJobDownloadByRoot()
		if workAttempted {
			continue
		}
		// Perform any job to help download a chunk.
		workAttempted = w.managedPerformDownloadChunkJob()
		if workAttempted {
			continue
		}
		// Perform any job to help upload a chunk.
		workAttempted = w.managedPerformUploadChunkJob()
		if workAttempted {
			continue
		}

		// Create a timer and a drain function for the timer.
		cacheUpdateTimer := time.NewTimer(workerCacheUpdateFrequency)
		drainCacheTimer := func() {
			if !cacheUpdateTimer.Stop() {
				<-cacheUpdateTimer.C
			}
		}

		// Block until:
		//    + New work has been submitted
		//    + The cache timer fires
		//    + The worker is killed
		//    + The renter is stopped
		select {
		case <-w.wakeChan:
			drainCacheTimer()
			continue
		case <-cacheUpdateTimer.C:
			continue
		case <-w.killChan:
			drainCacheTimer()
			return
		case <-w.renter.tg.StopChan():
			drainCacheTimer()
			return
		}
	}
}

// newWorker will create and return a worker that is ready to receive jobs.
func (r *Renter) newWorker(hostPubKey types.SiaPublicKey, blockHeight types.BlockHeight, account *account) (*worker, error) {
	host, ok, err := r.hostDB.Host(hostPubKey)
	if err != nil {
		return nil, errors.AddContext(err, "could not find host entry")
	}
	if !ok {
		return nil, errors.New("host does not exist")
	}

	// set the balance target to 1SC
	//
	// TODO: check that the balance target  makes sense in function of the
	// amount of MDM programs it can run with that amount of money
	balanceTarget := types.SiacoinPrecision

	// calculate the host's mux address
	hostMuxAddress := fmt.Sprintf("%s:%s", host.NetAddress.Host(), host.HostExternalSettings.SiaMuxPort)

	return &worker{
		staticHostPubKey:     hostPubKey,
		staticHostPubKeyStr:  hostPubKey.String(),
		staticHostMuxAddress: hostMuxAddress,
		staticHostVersion:    host.Version,
		staticHostPrices:     hostPrices{},

		staticAccount:       account,
		staticBalanceTarget: balanceTarget,

		cachedBlockHeight: blockHeight,

		killChan: make(chan struct{}),
		wakeChan: make(chan struct{}, 1),
		renter:   r,
	}, nil
}

// threadedUpdateBlockHeightOnWorkers is called on consensus change and updates
// the (cached) blockheight on every individual worker.
func (r *Renter) threadedUpdateBlockHeightOnWorkers() {
	err := r.tg.Add()
	if err != nil {
		return
	}
	defer r.tg.Done()

	// grab the current block height and have all workers cache it
	blockHeight := r.cs.Height()
	for _, worker := range r.staticWorkerPool.managedWorkers() {
		worker.mu.Lock()
		worker.cachedBlockHeight = blockHeight
		worker.mu.Unlock()
	}
}

// managedTryRefillAccount will check if the account needs to be refilled
func (w *worker) managedTryRefillAccount() {
	if build.VersionCmp(w.staticHostVersion, renterHostProtocolVersion) < 0 {
		return
	}

	// set refill threshold at half the balance target
	threshold := w.staticBalanceTarget.Div64(2)
	refillAmount := threshold
	if w.staticAccount.managedTryRefill(threshold, refillAmount) {
		err := w.renter.tg.Add()
		if err != nil {
			w.renter.log.Println(err)
			return
		}
		go func() {
			var err error
			defer w.renter.tg.Done()
			defer w.staticAccount.managedCommitDeposit(refillAmount, err == nil)
			_, err = w.managedFundAccount(refillAmount)
			if err != nil {
				w.renter.log.Println("ERROR: failed to refill account", err)
				// TODO: add cooldown mechanism
			}
		}()
	}
}

// managedTryUpdatePriceTable will check if the price table needs to be updated
func (w *worker) managedTryUpdatePriceTable() {
	if build.VersionCmp(w.staticHostVersion, renterHostProtocolVersion) < 0 {
		return
	}

	if w.staticHostPrices.managedTryUpdate() {
		err := w.renter.tg.Add()
		if err != nil {
			w.renter.log.Println(err)
			return
		}
		go func() {
			defer w.renter.tg.Done()
			err := w.managedUpdatePriceTable()
			if err != nil {
				w.renter.log.Println("ERROR: failed to update price table", err)
				// TODO: add retry mechanism
			}
		}()
	}
}

// hostPrices is a helper struct that wraps a priceTable and adds its own
// separate mutex. It contains a flag 'updating' which indicates whether or not
// we are currently in the process of updating the pricetable. It also has an
// 'updateAt' property that is set when a price table is updated and is set to
// the time when we want to update the host prices.
type hostPrices struct {
	priceTable modules.RPCPriceTable
	updateAt   int64
	updating   bool
	staticMu   sync.Mutex
}

// managedPriceTable returns the current price table
func (hp *hostPrices) managedPriceTable() *modules.RPCPriceTable {
	hp.staticMu.Lock()
	defer hp.staticMu.Unlock()
	return &hp.priceTable
}

// managedTryUpdate is a helper function that checks whether or not we have to
// update the price table. If so, it flips the 'updating' flag on the hostPrices
// object to ensure we only try this once.
func (hp *hostPrices) managedTryUpdate() bool {
	hp.staticMu.Lock()
	defer hp.staticMu.Unlock()

	if !hp.updating && time.Now().Unix() >= hp.updateAt {
		hp.updating = true
		return true
	}
	return false
}

// managedUpdatePriceTable is a helper function that sets the priceTable and
// calculates when we should try and update the price table again. It flips the
// 'updating' flag to false.
func (hp *hostPrices) managedUpdatePriceTable(pt modules.RPCPriceTable) {
	hp.staticMu.Lock()
	defer hp.staticMu.Unlock()

	hp.priceTable = pt
	hp.updateAt = time.Now().Unix() + (pt.Expiry-time.Now().Unix())/2
	hp.updating = false
}
