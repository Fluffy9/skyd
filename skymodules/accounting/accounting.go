package accounting

import (
	"sync"
	"time"

	"gitlab.com/NebulousLabs/errors"
	"gitlab.com/NebulousLabs/threadgroup"
	"gitlab.com/skynetlabs/skyd/persist"
	"gitlab.com/skynetlabs/skyd/skymodules"
)

var (
	// errNilDeps is the error returned when no dependencies are provided
	errNilDeps = errors.New("dependencies cannot be nil")

	// errNilPersistDir is the error returned when no persistDir is provided
	errNilPersistDir = errors.New("persistDir cannot by blank")

	// errNilWallet is the error returned when the wallet is nil
	errNilWallet = errors.New("wallet cannot be nil")
)

// Accounting contains the information needed for providing accounting
// information about a Sia node.
type Accounting struct {
	// Modules whos accounting information is tracked
	staticFeeManager skymodules.FeeManager
	staticHost       skymodules.Host
	staticMiner      skymodules.Miner
	staticRenter     skymodules.Renter
	staticWallet     skymodules.Wallet

	// Accounting module settings
	persistence      persistence
	staticPersistDir string

	// Utilities
	staticAOP  *persist.AppendOnlyPersist
	staticDeps skymodules.Dependencies
	staticLog  *persist.Logger
	staticTG   threadgroup.ThreadGroup

	mu sync.Mutex
}

// NewCustomAccounting initializes the accounting module with custom
// dependencies
func NewCustomAccounting(fm skymodules.FeeManager, h skymodules.Host, m skymodules.Miner, r skymodules.Renter, w skymodules.Wallet, persistDir string, deps skymodules.Dependencies) (*Accounting, error) {
	// Check that at least the wallet is not nil
	if w == nil {
		return nil, errNilWallet
	}

	// Check required parameters
	if persistDir == "" {
		return nil, errNilPersistDir
	}
	if deps == nil {
		return nil, errNilDeps
	}

	// Initialize the accounting
	a := &Accounting{
		staticFeeManager: fm,
		staticHost:       h,
		staticMiner:      m,
		staticRenter:     r,
		staticWallet:     w,

		staticPersistDir: persistDir,

		staticDeps: deps,
	}

	// Initialize the persistence
	err := a.initPersist()
	if err != nil {
		return nil, errors.AddContext(err, "unable to initialize the persistence")
	}

	// Launch background thread to persist the accounting information
	if !a.staticDeps.Disrupt("DisablePersistLoop") {
		go a.callThreadedPersistAccounting()
	}
	return a, nil
}

// Accounting returns the current accounting information
func (a *Accounting) Accounting() (skymodules.AccountingInfo, error) {
	err := a.staticTG.Add()
	if err != nil {
		return skymodules.AccountingInfo{}, err
	}
	defer a.staticTG.Done()

	// Update the accounting information
	ai, err := a.callUpdateAccounting()
	if err != nil {
		return skymodules.AccountingInfo{}, errors.AddContext(err, "unable to update the accounting information")
	}

	return ai, nil
}

// Close closes the accounting module
//
// NOTE: It will not call close on any of the modules it is tracking. Those
// modules are responsible for closing themselves independently.
func (a *Accounting) Close() error {
	return a.staticTG.Stop()
}

// callUpdateAccounting updates the accounting information
func (a *Accounting) callUpdateAccounting() (skymodules.AccountingInfo, error) {
	var ai skymodules.AccountingInfo

	// Get Renter information
	//
	// NOTE: renter is optional so can be nil
	var renterErr error
	if a.staticRenter != nil {
		var spending skymodules.ContractorSpending
		spending, renterErr = a.staticRenter.PeriodSpending()
		if renterErr == nil {
			_, _, unspentUnallocated := spending.SpendingBreakdown()
			ai.Renter.UnspentUnallocated = unspentUnallocated
			ai.Renter.WithheldFunds = spending.WithheldFunds
		}
	}

	// Get Wallet information
	sc, sf, _, walletErr := a.staticWallet.ConfirmedBalance()
	if walletErr == nil {
		ai.Wallet.ConfirmedSiacoinBalance = sc
		ai.Wallet.ConfirmedSiafundBalance = sf
	}

	// Update the Accounting state
	err := errors.Compose(renterErr, walletErr)
	if err == nil {
		a.mu.Lock()
		a.persistence.Renter = ai.Renter
		a.persistence.Wallet = ai.Wallet
		a.persistence.Timestamp = time.Now().Unix()
		a.mu.Unlock()
	}
	return ai, err
}

// Enforce that Accounting satisfies the skymodules.Accounting interface.
var _ skymodules.Accounting = (*Accounting)(nil)