package renter

// TODO: The way it's currently written, I believe the worker will discard all
// async tasks until it has a price table and understands the account funding.
// The worker should probably have in it's early startup routine (before
// entering the work loop proper) a blocking attempt to get a PT and fill an
// account.

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

import (
	"fmt"
	"sync"
	"time"
	"unsafe"

	"gitlab.com/NebulousLabs/Sia/build"
	"gitlab.com/NebulousLabs/Sia/modules"
	"gitlab.com/NebulousLabs/Sia/types"

	"gitlab.com/NebulousLabs/errors"
)

type (
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
	worker struct {
		// atomicCache contains a pointer to the latest cache in the worker.
		// Atomics are used to minimze lock contention on the worker object.
		atomicCache      unsafe.Pointer // points to a workerCache object
		atomicPriceTable unsafe.Pointer // points to a workerPriceTable object

		// The host pub key also serves as an id for the worker, as there is only
		// one worker per host.
		staticHostFCID       types.FileContractID
		staticHostPubKey     types.SiaPublicKey
		staticHostPubKeyStr  string
		staticHostMuxAddress string

		// Download variables related to queuing work. They have a separate mutex to
		// minimize lock contention.
		downloadChunks              []*unfinishedDownloadChunk // Yet unprocessed work items.
		downloadMu                  sync.Mutex
		downloadTerminated          bool      // Has downloading been terminated for this worker?
		downloadConsecutiveFailures int       // How many failures in a row?
		downloadRecentFailure       time.Time // How recent was the last failure?

		// Job queues for the worker.
		staticFetchBackupsJobQueue   fetchBackupsJobQueue
		staticJobQueueDownloadByRoot jobQueueDownloadByRoot
		staticJobQueueHasSector      *jobQueueHasSector

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

		// The loop state contains information about the worker loop. It is
		// mostly atomic variables that the worker uses to ratelimit the
		// launching of async jobs.
		staticLoopState workerLoopState

		// Utilities.
		killChan chan struct{} // Worker will shut down if a signal is sent down this channel.
		mu       sync.Mutex
		renter   *Renter
		wakeChan chan struct{} // Worker will check queues if given a wake signal.
	}
)

// status returns the status of the worker.
func (w *worker) status() modules.WorkerStatus {
	downloadOnCoolDown := w.onDownloadCooldown()
	uploadOnCoolDown, uploadCoolDownTime := w.onUploadCooldown()

	var uploadCoolDownErr string
	if w.uploadRecentFailureErr != nil {
		uploadCoolDownErr = w.uploadRecentFailureErr.Error()
	}

	var accountBalance types.Currency
	if w.staticAccount != nil {
		w.staticAccount.managedAvailableBalance()
	}

	// Update the worker cache before returning a status.
	w.staticTryUpdateCache()
	cache := w.staticCache()
	return modules.WorkerStatus{
		// Contract Information
		ContractID:      cache.staticContractID,
		ContractUtility: cache.staticContractUtility,
		HostPubKey:      w.staticHostPubKey,

		// Download information
		DownloadOnCoolDown: downloadOnCoolDown,
		DownloadQueueSize:  len(w.downloadChunks),
		DownloadTerminated: w.downloadTerminated,

		// Upload information
		UploadCoolDownError: uploadCoolDownErr,
		UploadCoolDownTime:  uploadCoolDownTime,
		UploadOnCoolDown:    uploadOnCoolDown,
		UploadQueueSize:     len(w.unprocessedChunks),
		UploadTerminated:    w.uploadTerminated,

		// Ephemeral Account information
		AvailableBalance: accountBalance,
		BalanceTarget:    w.staticBalanceTarget,

		// Job Queues
		BackupJobQueueSize:       w.staticFetchBackupsJobQueue.managedLen(),
		DownloadRootJobQueueSize: w.staticJobQueueDownloadByRoot.managedLen(),
	}
}

// newWorker will create and return a worker that is ready to receive jobs.
func (r *Renter) newWorker(hostPubKey types.SiaPublicKey, hostFCID types.FileContractID) (*worker, error) {
	host, ok, err := r.hostDB.Host(hostPubKey)
	if err != nil {
		return nil, errors.AddContext(err, "could not find host entry")
	}
	if !ok {
		return nil, errors.New("host does not exist")
	}

	// open the account
	account, err := r.managedOpenAccount(hostPubKey)
	if err != nil {
		return nil, errors.AddContext(err, "could not open account")
	}

	// set the balance target to 1SC
	//
	// TODO: check that the balance target  makes sense in function of the
	// amount of MDM programs it can run with that amount of money
	balanceTarget := types.SiacoinPrecision

	// calculate the host's mux address
	hostMuxAddress := fmt.Sprintf("%s:%s", host.NetAddress.Host(), host.HostExternalSettings.SiaMuxPort)

	w := &worker{
		staticHostPubKey:     hostPubKey,
		staticHostPubKeyStr:  hostPubKey.String(),
		staticHostMuxAddress: hostMuxAddress,
		staticHostFCID:       hostFCID,

		staticAccount:       account,
		staticBalanceTarget: balanceTarget,

		// Initialize the read and write limits for the async worker tasks.
		// These may be updated in real time as the worker collects metrics
		// about itself.
		staticLoopState: workerLoopState{
			atomicReadDataLimit:  10e6,
			atomicWriteDataLimit: 10e6,
		},

		killChan: make(chan struct{}),
		wakeChan: make(chan struct{}, 1),
		renter:   r,
	}
	w.newPriceTable()
	w.newJobQueueHasSector()
	// Get the worker cache set up before returning the worker. This prvents a
	// race condition in some tests.
	if !w.staticTryUpdateCache() {
		return nil, errors.New("unable to build cache for worker")
	}
	return w, nil
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

// staticWake will wake the worker from sleeping. This should be called any time
// that a job is queued or a job completes.
func (w *worker) staticWake() {
	select {
	case w.wakeChan <- struct{}{}:
	default:
	}
}

// TODO: Should consider cooldowns.
func (w *worker) managedAccountNeedsRefill() bool {
	// check host version
	cache := w.staticCache()
	if build.VersionCmp(cache.staticHostVersion, modules.MinimumSupportedNewRenterHostProtocolVersion) < 0 {
		return false
	}

	// check if refill is necessary
	balance := w.staticAccount.managedAvailableBalance()
	if balance.Cmp(w.staticBalanceTarget.Div64(2)) >= 0 {
		return false
	}
	return true
}

// managedTryRefillAccount will check if the account needs to be refilled
//
// TODO: Needs to do cooldowns and error handling and stuff.
func (w *worker) managedRefillAccount() {
	// check if price table is valid
	if w.staticPriceTable().staticPriceTable.Expiry <= time.Now().Unix() {
		w.renter.log.Println("ERROR: failed to refill account, current price table is expired")
		return
	}

	// the account balance dropped to below half the balance target, refill
	balance := w.staticAccount.managedAvailableBalance()
	amount := w.staticBalanceTarget.Sub(balance)
	_, err := w.managedFundAccount(amount)
	if err != nil {
		w.renter.log.Println("ERROR: failed to refill account", err)
		// TODO: add cooldown mechanism
	}
	return
}
