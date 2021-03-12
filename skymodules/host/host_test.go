package host

import (
	"bytes"
	"encoding/json"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"gitlab.com/NebulousLabs/Sia/crypto"
	"gitlab.com/NebulousLabs/errors"
	"gitlab.com/NebulousLabs/fastrand"
	"gitlab.com/NebulousLabs/siamux"
	"gitlab.com/NebulousLabs/siamux/mux"
	"gitlab.com/skynetlabs/skyd/build"
	"gitlab.com/skynetlabs/skyd/persist"
	"gitlab.com/skynetlabs/skyd/skymodules"
	"gitlab.com/skynetlabs/skyd/skymodules/consensus"
	"gitlab.com/skynetlabs/skyd/skymodules/gateway"
	"gitlab.com/skynetlabs/skyd/skymodules/miner"

	// "gitlab.com/skynetlabs/skyd/skymodules/renter"
	"gitlab.com/NebulousLabs/Sia/types"
	"gitlab.com/skynetlabs/skyd/skymodules/transactionpool"
	"gitlab.com/skynetlabs/skyd/skymodules/wallet"
	siasync "gitlab.com/skynetlabs/skyd/sync"
)

const (
	// priceTableExpiryBuffer defines a buffer period that ensures the price
	// table is valid for at least as long as the buffer period when we consider
	// it valid. This ensures a call to `managedFetchPriceTable` does not return
	// a price table that expires the next second.
	priceTableExpiryBuffer = 15 * time.Second
)

// A hostTester is the helper object for host testing, including helper modules
// and methods for controlling synchronization.
type (
	closeFn func() error

	hostTester struct {
		mux *siamux.SiaMux

		cs        skymodules.ConsensusSet
		gateway   skymodules.Gateway
		miner     skymodules.TestMiner
		tpool     skymodules.TransactionPool
		wallet    skymodules.Wallet
		walletKey crypto.CipherKey

		host *Host

		persistDir string
	}
)

/*
// initRenting prepares the host tester for uploads and downloads by announcing
// the host to the network and performing other preparational tasks.
// initRenting takes a while because the renter needs to process the host
// announcement, requiring asynchronous network communication between the
// renter and host.
func (ht *hostTester) initRenting() error {
	if ht.renting {
		return nil
	}

	// Because the renting test takes a long time, it will fail if
	// testing.Short.
	if testing.Short() {
		return errors.New("cannot call initRenting in short tests")
	}

	// Announce the host.
	err := ht.host.Announce()
	if err != nil {
		return err
	}

	// Mine a block to get the announcement into the blockchain.
	_, err = ht.miner.AddBlock()
	if err != nil {
		return err
	}

	// Wait for the renter to see the host announcement.
	for i := 0; i < 50; i++ {
		time.Sleep(time.Millisecond * 100)
		if len(ht.renter.ActiveHosts()) != 0 {
			break
		}
	}
	if len(ht.renter.ActiveHosts()) == 0 {
		return errors.New("could not start renting in the host tester")
	}
	ht.renting = true
	return nil
}
*/

// initWallet creates a wallet key, initializes the host wallet, unlocks it,
// and then stores the key in the host tester.
func (ht *hostTester) initWallet() error {
	// Create the keys for the wallet and unlock it.
	key := crypto.GenerateSiaKey(crypto.TypeDefaultWallet)
	ht.walletKey = key
	_, err := ht.wallet.Encrypt(key)
	if err != nil {
		return err
	}
	err = ht.wallet.Unlock(key)
	if err != nil {
		return err
	}
	return nil
}

// blankHostTester creates a host tester where the modules are created but no
// extra initialization has been done, for example no blocks have been mined
// and the wallet keys have not been created.
func blankHostTester(name string) (*hostTester, error) {
	return blankMockHostTester(skymodules.ProdDependencies, name)
}

// blankMockHostTester creates a host tester where the modules are created but no
// extra initialization has been done, for example no blocks have been mined
// and the wallet keys have not been created.
func blankMockHostTester(d skymodules.Dependencies, name string) (*hostTester, error) {
	testdir := build.TempDir(skymodules.HostDir, name)

	// Create the siamux.
	siaMuxDir := filepath.Join(testdir, skymodules.SiaMuxDir)
	mux, err := skymodules.NewSiaMux(siaMuxDir, testdir, "localhost:0", "localhost:0")
	if err != nil {
		return nil, err
	}

	// Create the skymodules.
	g, err := gateway.New("localhost:0", false, filepath.Join(testdir, skymodules.GatewayDir))
	if err != nil {
		return nil, err
	}
	cs, errChan := consensus.New(g, false, filepath.Join(testdir, skymodules.ConsensusDir))
	if err := <-errChan; err != nil {
		return nil, err
	}
	tp, err := transactionpool.New(cs, g, filepath.Join(testdir, skymodules.TransactionPoolDir))
	if err != nil {
		return nil, err
	}
	w, err := wallet.New(cs, tp, filepath.Join(testdir, skymodules.WalletDir))
	if err != nil {
		return nil, err
	}
	m, err := miner.New(cs, tp, w, filepath.Join(testdir, skymodules.MinerDir))
	if err != nil {
		return nil, err
	}
	h, err := NewCustomHost(d, cs, g, tp, w, mux, "localhost:0", filepath.Join(testdir, skymodules.HostDir))
	if err != nil {
		return nil, err
	}
	/*
		r, err := renter.New(cs, w, tp, filepath.Join(testdir, skymodules.RenterDir))
		if err != nil {
			return nil, err
		}
	*/

	// Assemble all objects into a hostTester
	ht := &hostTester{
		mux: mux,

		cs:      cs,
		gateway: g,
		miner:   m,
		// renter:  r,
		tpool:  tp,
		wallet: w,

		host: h,

		persistDir: testdir,
	}

	return ht, nil
}

// newHostTester creates a host tester with an initialized wallet and money in
// that wallet.
func newHostTester(name string) (*hostTester, error) {
	return newMockHostTester(skymodules.ProdDependencies, name)
}

// newMockHostTester creates a host tester with an initialized wallet and money
// in that wallet, using the dependencies provided.
func newMockHostTester(d skymodules.Dependencies, name string) (*hostTester, error) {
	// Create a blank host tester.
	ht, err := blankMockHostTester(d, name)
	if err != nil {
		return nil, err
	}

	// Initialize the wallet and mine blocks until the wallet has money.
	err = ht.initWallet()
	if err != nil {
		return nil, err
	}
	for i := types.BlockHeight(0); i <= types.MaturityDelay; i++ {
		_, err = ht.miner.AddBlock()
		if err != nil {
			return nil, err
		}
	}

	// Create two storage folder for the host, one the minimum size and one
	// twice the minimum size.
	storageFolderOne := filepath.Join(ht.persistDir, "hostTesterStorageFolderOne")
	err = os.Mkdir(storageFolderOne, 0700)
	if err != nil {
		return nil, err
	}
	err = ht.host.AddStorageFolder(storageFolderOne, skymodules.SectorSize*64)
	if err != nil {
		return nil, err
	}
	storageFolderTwo := filepath.Join(ht.persistDir, "hostTesterStorageFolderTwo")
	err = os.Mkdir(storageFolderTwo, 0700)
	if err != nil {
		return nil, err
	}
	err = ht.host.AddStorageFolder(storageFolderTwo, skymodules.SectorSize*64*2)
	if err != nil {
		return nil, err
	}
	return ht, nil
}

// Close safely closes the hostTester. It panics if err != nil because there
// isn't a good way to errcheck when deferring a close.
func (ht *hostTester) Close() error {
	errs := []error{
		ht.host.Close(),
		ht.miner.Close(),
		ht.wallet.Close(),
		ht.tpool.Close(),
		ht.cs.Close(),
		ht.gateway.Close(),
		ht.mux.Close(),
	}
	if err := build.JoinErrors(errs, "; "); err != nil {
		panic(err)
	}
	return nil
}

// renterHostPair is a helper struct that contains a secret key, symbolizing the
// renter, a host and the id of the file contract they share.
type renterHostPair struct {
	staticAccountID  skymodules.AccountID
	staticAccountKey crypto.SecretKey
	staticFCID       types.FileContractID
	staticRenterSK   crypto.SecretKey
	staticRenterPK   types.SiaPublicKey
	staticRenterMux  *siamux.SiaMux
	staticHT         *hostTester

	pt       *skymodules.RPCPriceTable
	ptExpiry time.Time // keep track of when the price table is set to expire

	mdmMu sync.RWMutex
	mu    sync.Mutex
}

// newRenterHostPair creates a new host tester and returns a renter host pair,
// this pair is a helper struct that contains both the host and renter,
// represented by its secret key. This helper will create a storage
// obligation emulating a file contract between them.
func newRenterHostPair(name string) (*renterHostPair, error) {
	return newCustomRenterHostPair(name, skymodules.ProdDependencies)
}

// newCustomRenterHostPair creates a new host tester and returns a renter host
// pair, this pair is a helper struct that contains both the host and renter,
// represented by its secret key. This helper will create a storage obligation
// emulating a file contract between them. It is custom as it allows passing a
// set of dependencies.
func newCustomRenterHostPair(name string, deps skymodules.Dependencies) (*renterHostPair, error) {
	// setup host
	ht, err := newMockHostTester(deps, name)
	if err != nil {
		return nil, err
	}
	return newRenterHostPairCustomHostTester(ht)
}

// newRenterHostPairCustomHostTester returns a renter host pair, this pair is a
// helper struct that contains both the host and renter, represented by its
// secret key. This helper will create a storage obligation emulating a file
// contract between them. This method requires the caller to pass a hostTester
// opposed to creating one, which allows setting up multiple renters which each
// have a contract with the one host.
func newRenterHostPairCustomHostTester(ht *hostTester) (*renterHostPair, error) {
	// create a renter key pair
	sk, pk := crypto.GenerateKeyPair()
	renterPK := types.SiaPublicKey{
		Algorithm: types.SignatureEd25519,
		Key:       pk[:],
	}

	// setup storage obligation (emulating a renter creating a contract)
	so, err := ht.newTesterStorageObligation()
	if err != nil {
		return nil, errors.AddContext(err, "unable to make the new tester storage obligation")
	}
	so, err = ht.addNoOpRevision(so, renterPK)
	if err != nil {
		return nil, errors.AddContext(err, "unable to add noop revision")
	}
	ht.host.managedLockStorageObligation(so.id())
	err = ht.host.managedAddStorageObligation(so)
	if err != nil {
		return nil, errors.AddContext(err, "unable to add the storage obligation")
	}
	ht.host.managedUnlockStorageObligation(so.id())

	// prepare an EA without funding it.
	accountKey, accountID := prepareAccount()

	// prepare a siamux for the renter
	renterMuxDir := filepath.Join(ht.persistDir, "rentermux")
	if err := os.MkdirAll(renterMuxDir, 0700); err != nil {
		return nil, errors.AddContext(err, "unable to mkdirall")
	}
	muxLogger, err := persist.NewFileLogger(filepath.Join(renterMuxDir, "siamux.log"))
	if err != nil {
		return nil, errors.AddContext(err, "unable to create mux logger")
	}
	renterMux, err := siamux.New("127.0.0.1:0", "127.0.0.1:0", muxLogger.Logger, renterMuxDir)
	if err != nil {
		return nil, errors.AddContext(err, "unable to create renter mux")
	}

	pair := &renterHostPair{
		staticAccountID:  accountID,
		staticAccountKey: accountKey,
		staticRenterSK:   sk,
		staticRenterPK:   renterPK,
		staticRenterMux:  renterMux,
		staticFCID:       so.id(),
		staticHT:         ht,
	}

	// fetch a price table
	err = pair.managedUpdatePriceTable(true)
	if err != nil {
		return nil, errors.AddContext(err, "unable to update price table")
	}

	// sanity check to verify the refund account used to update the PT is empty
	// to ensure the test starts with a clean slate
	am := pair.staticHT.host.staticAccountManager
	balance := am.callAccountBalance(pair.staticAccountID)
	if !balance.IsZero() {
		return nil, errors.New("account balance was not zero after initialising a renter host pair")
	}

	return pair, nil
}

// Close closes the underlying host tester.
func (p *renterHostPair) Close() error {
	err1 := p.staticRenterMux.Close()
	err2 := p.staticHT.Close()
	return errors.Compose(err1, err2)
}

// executeProgramResponse is a helper struct that wraps the
// RPCExecuteProgramResponse together with the output data
type executeProgramResponse struct {
	skymodules.RPCExecuteProgramResponse
	Output []byte
}

// managedExecuteProgram executes an MDM program on the host using an EA payment
// and returns the responses received by the host. A failure to execute an
// instruction won't result in an error. Instead the returned responses need to
// be inspected for that depending on the testcase.
func (p *renterHostPair) managedExecuteProgram(epr skymodules.RPCExecuteProgramRequest, programData []byte, budget types.Currency, updatePriceTable, finalize bool) (_ []executeProgramResponse, _ mux.BandwidthLimit, err error) {
	// Only allow a single write program or multiple read programs to run in
	// parallel. A production worker will have better locking than this but
	// since we just mock the renter this is used for unit testing the host.
	if epr.Program.ReadOnly() {
		p.mdmMu.RLock()
		defer p.mdmMu.RUnlock()
	} else {
		p.mdmMu.Lock()
		defer p.mdmMu.Unlock()
	}

	pt := p.managedPriceTable()
	if updatePriceTable {
		pt, err = p.managedFetchPriceTable()
		if err != nil {
			return nil, nil, err
		}
	}

	// create a buffer to optimise our writes
	buffer := bytes.NewBuffer(nil)

	// Write the specifier.
	err = skymodules.RPCWrite(buffer, skymodules.RPCExecuteProgram)
	if err != nil {
		return nil, nil, err
	}

	// Write the pricetable uid.
	err = skymodules.RPCWrite(buffer, pt.UID)
	if err != nil {
		return nil, nil, err
	}

	// Send the payment request.
	err = skymodules.RPCWrite(buffer, skymodules.PaymentRequest{Type: skymodules.PayByEphemeralAccount})
	if err != nil {
		return nil, nil, err
	}

	// Send the payment details.
	pbear := skymodules.NewPayByEphemeralAccountRequest(p.staticAccountID, pt.HostBlockHeight, budget, p.staticAccountKey)
	err = skymodules.RPCWrite(buffer, pbear)
	if err != nil {
		return nil, nil, err
	}

	// Send the execute program request.
	err = skymodules.RPCWrite(buffer, epr)
	if err != nil {
		return nil, nil, err
	}

	// Send the programData.
	_, err = buffer.Write(programData)
	if err != nil {
		return nil, nil, err
	}

	// create stream
	stream := p.managedNewStream()
	defer func() {
		err = errors.Compose(err, stream.Close())
	}()

	// Get the limit to track bandwidth.
	limit := stream.Limit()

	// write contents of the buffer to the stream
	_, err = stream.Write(buffer.Bytes())
	if err != nil {
		return nil, limit, err
	}

	// Read the cancellation token.
	var ct skymodules.MDMCancellationToken
	err = skymodules.RPCRead(stream, &ct)
	if err != nil {
		return nil, limit, err
	}

	// Read the responses.
	responses := make([]executeProgramResponse, len(epr.Program))
	for i := range epr.Program {
		// Read the response.
		err = skymodules.RPCRead(stream, &responses[i])
		if err != nil {
			return nil, limit, err
		}

		// Read the output data.
		outputLen := responses[i].OutputLength
		responses[i].Output = make([]byte, outputLen, outputLen)
		_, err = io.ReadFull(stream, responses[i].Output)
		if err != nil {
			return nil, limit, err
		}

		// If the response contains an error we are done.
		if responses[i].Error != nil {
			return responses, limit, nil
		}
	}

	// If the program was not readonly, the host expects a signed revision.
	if !epr.Program.ReadOnly() && finalize {
		lastOutput := responses[len(responses)-1]
		err = p.managedFinalizeWriteProgram(stream, lastOutput, p.staticHT.host.BlockHeight())
		if err != nil {
			return nil, limit, err
		}
	}

	// when we purposefully don't finalize, we can't wait for the host to close
	// the stream.
	if !finalize {
		return responses, limit, nil
	}

	// The next read should return io.EOF since the host closes the connection
	// after the RPC is done.
	err = skymodules.RPCRead(stream, struct{}{})
	if !errors.Contains(err, io.ErrClosedPipe) {
		return nil, limit, err
	}
	return responses, limit, nil
}

// managedFetchPriceTable returns the latest price table, if that price table is
// expired it will fetch a new one from the host.
func (p *renterHostPair) managedFetchPriceTable() (*skymodules.RPCPriceTable, error) {
	p.mu.Lock()
	expired := time.Now().Add(priceTableExpiryBuffer).After(p.ptExpiry)
	p.mu.Unlock()

	if expired {
		if err := p.managedUpdatePriceTable(true); err != nil {
			return nil, err
		}
	}
	return p.managedPriceTable(), nil
}

// managedFundEphemeralAccount will deposit the given amount in the pair's
// ephemeral account using the pair's file contract to provide payment and the
// given price table.
func (p *renterHostPair) managedFundEphemeralAccount(amount types.Currency, updatePriceTable bool) (_ skymodules.FundAccountResponse, err error) {
	pt := p.managedPriceTable()
	if updatePriceTable {
		pt, err = p.managedFetchPriceTable()
		if err != nil {
			return skymodules.FundAccountResponse{}, err
		}
	}

	// create stream
	stream := p.managedNewStream()
	defer func() {
		err = errors.Compose(err, stream.Close())
	}()

	// Write RPC ID.
	err = skymodules.RPCWrite(stream, skymodules.RPCFundAccount)
	if err != nil {
		return skymodules.FundAccountResponse{}, err
	}

	// Write price table id.
	err = skymodules.RPCWrite(stream, pt.UID)
	if err != nil {
		return skymodules.FundAccountResponse{}, err
	}

	// send fund account request
	req := skymodules.FundAccountRequest{Account: p.staticAccountID}
	err = skymodules.RPCWrite(stream, req)
	if err != nil {
		return skymodules.FundAccountResponse{}, err
	}

	// Pay by contract.
	err = p.managedPayByContract(stream, amount, skymodules.ZeroAccountID)
	if err != nil {
		return skymodules.FundAccountResponse{}, err
	}

	// receive FundAccountResponse
	var resp skymodules.FundAccountResponse
	err = skymodules.RPCRead(stream, &resp)
	if err != nil {
		return skymodules.FundAccountResponse{}, err
	}
	return resp, nil
}

// managedNewStream opens a stream to the pair's host and returns it
func (p *renterHostPair) managedNewStream() siamux.Stream {
	pk := skymodules.SiaPKToMuxPK(p.staticHT.host.publicKey)
	address := p.staticHT.host.ExternalSettings().SiaMuxAddress()
	subscriber := skymodules.HostSiaMuxSubscriberName

	stream, err := p.staticRenterMux.NewStream(subscriber, address, pk)
	if err != nil {
		panic(err)
	}
	return stream
}

// managedPayByContract is a helper that creates a payment revision and uses it
// to pay the specified amount. It will also verify the signature of the
// returned response.
func (p *renterHostPair) managedPayByContract(stream siamux.Stream, amount types.Currency, refundAccount skymodules.AccountID) error {
	// create the revision.
	revision, sig, err := p.managedEAFundRevision(amount)
	if err != nil {
		return err
	}

	// send PaymentRequest & PayByContractRequest
	pRequest := skymodules.PaymentRequest{Type: skymodules.PayByContract}
	pbcRequest := newPayByContractRequest(revision, sig, refundAccount)
	err = skymodules.RPCWriteAll(stream, pRequest, pbcRequest)
	if err != nil {
		return err
	}

	// receive PayByContractResponse
	var payByResponse skymodules.PayByContractResponse
	err = skymodules.RPCRead(stream, &payByResponse)
	if err != nil {
		return err
	}

	// verify the host signature
	if err := crypto.VerifyHash(crypto.HashAll(revision), p.staticHT.host.secretKey.PublicKey(), payByResponse.Signature); err != nil {
		return errors.New("could not verify host signature")
	}
	return nil
}

// managedPayByEphemeralAccount is a helper that makes payment using the pair's
// EA.
func (p *renterHostPair) managedPayByEphemeralAccount(stream siamux.Stream, amount types.Currency) error {
	// Send the payment request.
	err := skymodules.RPCWrite(stream, skymodules.PaymentRequest{Type: skymodules.PayByEphemeralAccount})
	if err != nil {
		return err
	}

	// Send the payment details.
	pbear := skymodules.NewPayByEphemeralAccountRequest(p.staticAccountID, p.pt.HostBlockHeight, amount, p.staticAccountKey)
	err = skymodules.RPCWrite(stream, pbear)
	if err != nil {
		return err
	}

	return nil
}

// managedFinalizeWriteProgram finalizes a write program by conducting an
// additional handshake which signs a new revision.
func (p *renterHostPair) managedFinalizeWriteProgram(stream siamux.Stream, lastOutput executeProgramResponse, bh types.BlockHeight) error {
	// Get the latest revision.
	updated, err := p.staticHT.host.managedGetStorageObligation(p.staticFCID)
	if err != nil {
		return err
	}
	recent, err := updated.recentRevision()
	if err != nil {
		return err
	}

	// Construct the new revision.
	transfer := lastOutput.AdditionalCollateral.Add(lastOutput.FailureRefund)
	newRevision, err := recent.ExecuteProgramRevision(recent.NewRevisionNumber+1, transfer, lastOutput.NewMerkleRoot, lastOutput.NewSize)
	if err != nil {
		return err
	}
	newValidProofValues := make([]types.Currency, len(newRevision.NewValidProofOutputs))
	for i := range newRevision.NewValidProofOutputs {
		newValidProofValues[i] = newRevision.NewValidProofOutputs[i].Value
	}
	newMissedProofValues := make([]types.Currency, len(newRevision.NewMissedProofOutputs))
	for i := range newRevision.NewMissedProofOutputs {
		newMissedProofValues[i] = newRevision.NewMissedProofOutputs[i].Value
	}

	// Sign revision.
	renterSig := p.managedSign(newRevision)

	// Prepare the request.
	req := skymodules.RPCExecuteProgramRevisionSigningRequest{
		Signature:            renterSig[:],
		NewRevisionNumber:    newRevision.NewRevisionNumber,
		NewValidProofValues:  newValidProofValues,
		NewMissedProofValues: newMissedProofValues,
	}

	// Send request.
	err = skymodules.RPCWrite(stream, req)
	if err != nil {
		return errors.AddContext(err, "managedFinalizeWriteProgram: RPCWrite failed")
	}

	// Receive response.
	var resp skymodules.RPCExecuteProgramRevisionSigningResponse
	err = skymodules.RPCRead(stream, &resp)
	if err != nil {
		return errors.AddContext(err, "managedFinalizeWriteProgram: RPCRead failed")
	}

	// check host signature
	hs := types.TransactionSignature{
		ParentID:       crypto.Hash(newRevision.ParentID),
		PublicKeyIndex: 1,
		CoveredFields: types.CoveredFields{
			FileContractRevisions: []uint64{0},
		},
		Signature: resp.Signature,
	}
	rs := types.TransactionSignature{
		ParentID:       crypto.Hash(newRevision.ParentID),
		PublicKeyIndex: 0,
		CoveredFields: types.CoveredFields{
			FileContractRevisions: []uint64{0},
		},
		Signature: req.Signature,
	}
	txn := types.Transaction{
		FileContractRevisions: []types.FileContractRevision{newRevision},
		TransactionSignatures: []types.TransactionSignature{rs, hs},
	}
	err = skymodules.VerifyFileContractRevisionTransactionSignatures(newRevision, txn.TransactionSignatures, bh)
	if err != nil {
		return errors.AddContext(err, "signature verification failed")
	}
	return nil
}

// managedEAFundRevision returns a new revision that transfer the given amount
// to the host. Returns the payment revision together with a signature signed by
// the pair's renter.
func (p *renterHostPair) managedEAFundRevision(amount types.Currency) (types.FileContractRevision, crypto.Signature, error) {
	updated, err := p.staticHT.host.managedGetStorageObligation(p.staticFCID)
	if err != nil {
		return types.FileContractRevision{}, crypto.Signature{}, err
	}

	recent, err := updated.recentRevision()
	if err != nil {
		return types.FileContractRevision{}, crypto.Signature{}, err
	}

	rev, err := recent.EAFundRevision(amount)
	if err != nil {
		return types.FileContractRevision{}, crypto.Signature{}, err
	}

	return rev, p.managedSign(rev), nil
}

// managedPriceTable returns the latest price table
func (p *renterHostPair) managedPriceTable() *skymodules.RPCPriceTable {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.pt
}

// managedRecentHostRevision returns the most recent revision the host has
// stored for the pair's contract.
func (p *renterHostPair) managedRecentHostRevision() (types.FileContractRevision, error) {
	so, err := p.managedStorageObligation()
	if err != nil {
		return types.FileContractRevision{}, err
	}
	return so.recentRevision()
}

// managedStorageObligation returns the host's storage obligation for the pairs
// contract.
func (p *renterHostPair) managedStorageObligation() (storageObligation, error) {
	return p.staticHT.host.managedGetStorageObligation(p.staticFCID)
}

// managedSign returns the renter's signature of the given revision
func (p *renterHostPair) managedSign(rev types.FileContractRevision) crypto.Signature {
	signedTxn := types.Transaction{
		FileContractRevisions: []types.FileContractRevision{rev},
		TransactionSignatures: []types.TransactionSignature{{
			ParentID:       crypto.Hash(rev.ParentID),
			CoveredFields:  types.CoveredFields{FileContractRevisions: []uint64{0}},
			PublicKeyIndex: 0,
		}},
	}
	hash := signedTxn.SigHash(0, p.staticHT.host.BlockHeight())
	return crypto.SignHash(hash, p.staticRenterSK)
}

// AccountBalance returns the account balance of the specified account.
func (p *renterHostPair) managedAccountBalance(payByFC bool, fundAmt types.Currency, fundAcc, balanceAcc skymodules.AccountID) (_ types.Currency, err error) {
	stream := p.managedNewStream()
	defer func() {
		err = errors.Compose(err, stream.Close())
	}()

	// Fetch the price table.
	pt, err := p.managedFetchPriceTable()
	if err != nil {
		return types.ZeroCurrency, err
	}

	// initiate the RPC
	err = skymodules.RPCWrite(stream, skymodules.RPCAccountBalance)
	if err != nil {
		return types.ZeroCurrency, err
	}

	// Write the pricetable uid.
	err = skymodules.RPCWrite(stream, pt.UID)
	if err != nil {
		return types.ZeroCurrency, err
	}

	// provide payment
	if payByFC {
		err = p.managedPayByContract(stream, fundAmt, fundAcc)
		if err != nil {
			return types.ZeroCurrency, err
		}
	} else {
		err = p.managedPayByEphemeralAccount(stream, fundAmt)
		if err != nil {
			return types.ZeroCurrency, err
		}
	}

	// send the request.
	err = skymodules.RPCWrite(stream, skymodules.AccountBalanceRequest{
		Account: balanceAcc,
	})
	if err != nil {
		return types.ZeroCurrency, err
	}

	// read the response.
	var abr skymodules.AccountBalanceResponse
	err = skymodules.RPCRead(stream, &abr)
	if err != nil {
		return types.ZeroCurrency, err
	}

	// expect clean stream close
	err = skymodules.RPCRead(stream, struct{}{})
	if !errors.Contains(err, io.ErrClosedPipe) {
		return types.ZeroCurrency, err
	}

	return abr.Balance, nil
}

// managedBeginSubscription begins a subscription on a new stream and returns
// it.
func (p *renterHostPair) managedBeginSubscription(amount types.Currency, subscriber types.Specifier) (_ siamux.Stream, err error) {
	stream := p.managedNewStream()
	defer func() {
		if err != nil {
			err = errors.Compose(err, stream.Close())
		}
	}()

	// Fetch the price table.
	pt, err := p.managedFetchPriceTable()
	if err != nil {
		return nil, err
	}

	return stream, skymodules.RPCBeginSubscription(stream, p.staticHT.host.publicKey, pt, p.staticAccountID, p.staticAccountKey, amount, pt.HostBlockHeight, subscriber)
}

// managedLatestRevision performs a RPCLatestRevision to get the latest revision
// for the contract with fcid from the host.
func (p *renterHostPair) managedLatestRevision(payByFC bool, fundAmt types.Currency, fundAcc skymodules.AccountID, fcid types.FileContractID) (_ types.FileContractRevision, err error) {
	stream := p.managedNewStream()
	defer func() {
		err = errors.Compose(err, stream.Close())
	}()

	// Fetch the price table.
	pt, err := p.managedFetchPriceTable()
	if err != nil {
		return types.FileContractRevision{}, err
	}

	// initiate the RPC
	err = skymodules.RPCWrite(stream, skymodules.RPCLatestRevision)
	if err != nil {
		return types.FileContractRevision{}, err
	}

	// send the request.
	err = skymodules.RPCWrite(stream, skymodules.RPCLatestRevisionRequest{
		FileContractID: fcid,
	})
	if err != nil {
		return types.FileContractRevision{}, err
	}

	// read the response.
	var lrr skymodules.RPCLatestRevisionResponse
	err = skymodules.RPCRead(stream, &lrr)
	if err != nil {
		return types.FileContractRevision{}, err
	}

	// Write the pricetable uid.
	err = skymodules.RPCWrite(stream, pt.UID)
	if err != nil {
		return types.FileContractRevision{}, err
	}

	// provide payment
	if payByFC {
		err = p.managedPayByContract(stream, fundAmt, fundAcc)
		if err != nil {
			return types.FileContractRevision{}, err
		}
	} else {
		err = p.managedPayByEphemeralAccount(stream, fundAmt)
		if err != nil {
			return types.FileContractRevision{}, err
		}
	}

	// expect clean stream close
	err = skymodules.RPCRead(stream, struct{}{})
	if !errors.Contains(err, io.ErrClosedPipe) {
		return types.FileContractRevision{}, err
	}

	return lrr.Revision, nil
}

// AccountBalance returns the account balance of the renter's EA on the host.
func (p *renterHostPair) AccountBalance(payByFC bool) (types.Currency, error) {
	return p.managedAccountBalance(payByFC, p.pt.AccountBalanceCost, p.staticAccountID, p.staticAccountID)
}

// BeginSubscription starts the subscription loop and returns the stream.
func (p *renterHostPair) BeginSubscription(budget types.Currency, subscriber types.Specifier) (siamux.Stream, error) {
	return p.managedBeginSubscription(budget, subscriber)
}

// LatestRevision performs a RPCLatestRevision to get the latest revision for
// the contract from the host.
func (p *renterHostPair) LatestRevision(payByFC bool) (types.FileContractRevision, error) {
	return p.managedLatestRevision(payByFC, p.pt.LatestRevisionCost, p.staticAccountID, p.staticFCID)
}

// StopSubscription gracefully stops a subscription session.
func (p *renterHostPair) StopSubscription(stream siamux.Stream) error {
	return skymodules.RPCStopSubscription(stream)
}

// SubscribeToRV subscribes to the given publickey/tweak pair.
func (p *renterHostPair) SubcribeToRV(stream siamux.Stream, pt *skymodules.RPCPriceTable, pubkey types.SiaPublicKey, tweak crypto.Hash) (*skymodules.SignedRegistryValue, error) {
	rvs, err := skymodules.RPCSubscribeToRVs(stream, []skymodules.RPCRegistrySubscriptionRequest{{
		PubKey: pubkey,
		Tweak:  tweak,
	}})
	if err != nil {
		return nil, err
	}

	// Check response.
	var rv *skymodules.SignedRegistryValue
	if len(rvs) > 1 {
		build.Critical("more responses than subscribed to values")
	} else if len(rvs) == 1 {
		rv = &rvs[0].Entry
	}
	return rv, nil
}

// UnsubscribeFromRV unsubscribes from the given publickey/tweak pair.
func (p *renterHostPair) UnsubcribeFromRV(stream siamux.Stream, pt *skymodules.RPCPriceTable, pubkey types.SiaPublicKey, tweak crypto.Hash) error {
	return skymodules.RPCUnsubscribeFromRVs(stream, []skymodules.RPCRegistrySubscriptionRequest{{
		PubKey: pubkey,
		Tweak:  tweak,
	}})
}

// FundSubscription pays the host to increase the subscription budget.
func (p *renterHostPair) FundSubscription(stream siamux.Stream, fundAmt types.Currency) error {
	return skymodules.RPCFundSubscription(stream, p.staticHT.host.publicKey, p.staticAccountID, p.staticAccountKey, p.pt.HostBlockHeight, fundAmt)
}

// UpdatePriceTable runs the UpdatePriceTableRPC on the host and sets the price
// table on the pair
func (p *renterHostPair) managedUpdatePriceTable(payByFC bool) (err error) {
	stream := p.managedNewStream()
	defer func() {
		err = errors.Compose(err, stream.Close())
	}()

	// initiate the RPC
	err = skymodules.RPCWrite(stream, skymodules.RPCUpdatePriceTable)
	if err != nil {
		return err
	}

	// receive the price table response
	var update skymodules.RPCUpdatePriceTableResponse
	err = skymodules.RPCRead(stream, &update)
	if err != nil {
		return err
	}

	var pt skymodules.RPCPriceTable
	err = json.Unmarshal(update.PriceTableJSON, &pt)
	if err != nil {
		return err
	}

	if payByFC {
		err = p.managedPayByContract(stream, pt.UpdatePriceTableCost, p.staticAccountID)
		if err != nil {
			return err
		}
	} else {
		err = p.managedPayByEphemeralAccount(stream, pt.UpdatePriceTableCost)
		if err != nil {
			return err
		}
	}

	// read the tracked response
	var tracked skymodules.RPCTrackedPriceTableResponse
	err = skymodules.RPCRead(stream, &tracked)
	if err != nil {
		return err
	}

	// update the price table
	p.mu.Lock()
	p.pt = &pt
	p.ptExpiry = time.Now().Add(pt.Validity)
	p.mu.Unlock()

	return nil
}

// TestHostInitialization checks that the host initializes to sensible default
// values.
func TestHostInitialization(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	t.Parallel()

	// create a blank host tester
	ht, err := blankHostTester(t.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := ht.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	// verify its initial block height is zero
	if ht.host.blockHeight != 0 {
		t.Fatal("host initialized to the wrong block height")
	}

	// verify its RPC price table was properly initialised
	ht.host.staticPriceTables.mu.RLock()
	defer ht.host.staticPriceTables.mu.RUnlock()
	if reflect.DeepEqual(ht.host.staticPriceTables.current, skymodules.RPCPriceTable{}) {
		t.Fatal("RPC price table wasn't initialized")
	}
}

// TestHostRegistry tests that changing the internal settings of the host will
// update the registry as well.
func TestHostRegistry(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	t.Parallel()
	ht, err := newHostTester(t.Name())
	if err != nil {
		t.Fatal(err)
	}
	h := ht.host
	r := h.staticRegistry

	// The registry should be disabled by default.
	is := h.managedInternalSettings()
	if is.RegistrySize != 0 {
		t.Fatal("registry size should be 0 by default")
	}
	if r.Len() != 0 || r.Cap() != 0 {
		t.Fatal("registry len and cap should be 0")
	}

	// Update the internal settings.
	is.RegistrySize = 128 * skymodules.RegistryEntrySize
	err = h.SetInternalSettings(is)
	if err != nil {
		t.Fatal(err)
	}
	if r.Len() != 0 || r.Cap() != 128 {
		t.Fatal("truncate wasn't called on registry", r.Len(), r.Cap())
	}

	// Add 64 entries.
	for i := 0; i < 64; i++ {
		sk, pk := crypto.GenerateKeyPair()
		var spk types.SiaPublicKey
		spk.Algorithm = types.SignatureEd25519
		spk.Key = pk[:]
		var tweak crypto.Hash
		fastrand.Read(tweak[:])
		rv := skymodules.NewRegistryValue(tweak, fastrand.Bytes(skymodules.RegistryDataSize), 0).Sign(sk)
		_, err := h.RegistryUpdate(rv, spk, 1337)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Check registry.
	if r.Len() != 64 || r.Cap() != 128 {
		t.Fatal("truncate wasn't called on registry", r.Len(), r.Cap())
	}

	// Try truncating below that. Should round up to 64 entries.
	is.RegistrySize = 64*skymodules.RegistryEntrySize - 1
	err = h.SetInternalSettings(is)
	if err != nil {
		t.Fatal(err)
	}
	if r.Len() != 64 || r.Cap() != 64 {
		t.Fatal("truncate wasn't called on registry", r.Len(), r.Cap())
	}

	// Move to new location.
	dst := filepath.Join(ht.persistDir, "newreg.dat")
	is.CustomRegistryPath = dst
	err = h.SetInternalSettings(is)
	if err != nil {
		t.Fatal(err)
	}

	// Check that file exists and the old one doesn't.
	if _, err := os.Stat(dst); err != nil {
		t.Fatal(err)
	}
	defaultPath := filepath.Join(ht.host.persistDir, skymodules.HostRegistryFile)
	if _, err := os.Stat(defaultPath); !os.IsNotExist(err) {
		t.Fatal(err)
	}

	// Close host and restart it.
	err = ht.host.Close()
	if err != nil {
		t.Fatal(err)
	}
	h, err = New(ht.cs, ht.gateway, ht.tpool, ht.wallet, ht.mux, "localhost:0", filepath.Join(ht.persistDir, skymodules.HostDir))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := h.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	r = h.staticRegistry

	// Check registry.
	if r.Len() != 64 || r.Cap() != 64 {
		t.Fatal("truncate wasn't called on registry", r.Len(), r.Cap())
	}

	// Clear registry.
	_, err = r.Prune(types.BlockHeight(math.MaxUint64))
	if err != nil {
		t.Fatal(err)
	}
	if r.Len() != 0 || r.Cap() != 64 {
		t.Fatal("clearing wasn't successful", r.Len(), r.Cap())
	}

	// Set the size back to 0.
	is.RegistrySize = 0
	err = h.SetInternalSettings(is)
	if err != nil {
		t.Fatal(err)
	}
	if r.Len() != 0 || r.Cap() != 0 {
		t.Fatal("truncate wasn't called on registry")
	}

	// Move registry back to default.
	is.CustomRegistryPath = ""
	err = h.SetInternalSettings(is)
	if err != nil {
		t.Fatal(err)
	}

	// Check that registry exists at default again.
	if _, err := os.Stat(dst); !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if _, err := os.Stat(defaultPath); err != nil {
		t.Fatal(err)
	}
}

// TestHostMultiClose checks that the host returns an error if Close is called
// multiple times on the host.
func TestHostMultiClose(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	t.Parallel()
	ht, err := newHostTester("TestHostMultiClose")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := ht.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	err = ht.host.Close()
	if err != nil {
		t.Fatal(err)
	}
	err = ht.host.Close()
	if !errors.Contains(err, siasync.ErrStopped) {
		t.Fatal(err)
	}
	err = ht.host.Close()
	if !errors.Contains(err, siasync.ErrStopped) {
		t.Fatal(err)
	}
	// Set ht.host to something non-nil - nil was returned because startup was
	// incomplete. If ht.host is nil at the end of the function, the ht.Close()
	// operation will fail.
	ht.host, err = NewCustomHost(skymodules.ProdDependencies, ht.cs, ht.gateway, ht.tpool, ht.wallet, ht.mux, "localhost:0", filepath.Join(ht.persistDir, skymodules.HostDir))
	if err != nil {
		t.Fatal(err)
	}
}

// TestNilValues tries initializing the host with nil values.
func TestNilValues(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	t.Parallel()
	ht, err := blankHostTester("TestStartupRescan")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := ht.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	hostDir := filepath.Join(ht.persistDir, skymodules.HostDir)
	_, err = New(nil, ht.gateway, ht.tpool, ht.wallet, ht.mux, "localhost:0", hostDir)
	if !errors.Contains(err, errNilCS) {
		t.Fatal("could not trigger errNilCS")
	}
	_, err = New(ht.cs, nil, ht.tpool, ht.wallet, ht.mux, "localhost:0", hostDir)
	if !errors.Contains(err, errNilGateway) {
		t.Fatal("Could not trigger errNilGateay")
	}
	_, err = New(ht.cs, ht.gateway, nil, ht.wallet, ht.mux, "localhost:0", hostDir)
	if !errors.Contains(err, errNilTpool) {
		t.Fatal("could not trigger errNilTpool")
	}
	_, err = New(ht.cs, ht.gateway, ht.tpool, nil, ht.mux, "localhost:0", hostDir)
	if !errors.Contains(err, errNilWallet) {
		t.Fatal("Could not trigger errNilWallet")
	}
}

// TestRenterHostPair tests the newRenterHostPair constructor
func TestRenterHostPair(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	t.Parallel()

	rhp, err := newRenterHostPair(t.Name())
	if err != nil {
		t.Fatal(err)
	}
	err = rhp.Close()
	if err != nil {
		t.Fatal(err)
	}
}

// TestSetAndGetInternalSettings checks that the functions for interacting with
// the host's internal settings object are working as expected.
func TestSetAndGetInternalSettings(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	t.Parallel()

	ht, err := newHostTester("TestSetAndGetInternalSettings")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := ht.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	// Check the default settings get returned at first call.
	settings := ht.host.InternalSettings()
	if settings.AcceptingContracts != false {
		t.Error("settings retrieval did not return default value")
	}
	if settings.MaxDuration != skymodules.DefaultMaxDuration {
		t.Error("settings retrieval did not return default value")
	}
	if settings.MaxDownloadBatchSize != uint64(skymodules.DefaultMaxDownloadBatchSize) {
		t.Error("settings retrieval did not return default value")
	}
	if settings.MaxReviseBatchSize != uint64(skymodules.DefaultMaxReviseBatchSize) {
		t.Error("settings retrieval did not return default value")
	}
	if settings.NetAddress != "" {
		t.Error("settings retrieval did not return default value")
	}
	if settings.WindowSize != skymodules.DefaultWindowSize {
		t.Error("settings retrieval did not return default value")
	}
	if !settings.Collateral.Equals(skymodules.DefaultCollateral) {
		t.Error("settings retrieval did not return default value")
	}
	if !settings.CollateralBudget.Equals(defaultCollateralBudget) {
		t.Error("settings retrieval did not return default value")
	}
	if !settings.MaxCollateral.Equals(skymodules.DefaultMaxCollateral) {
		t.Error("settings retrieval did not return default value")
	}
	if !settings.MinContractPrice.Equals(skymodules.DefaultContractPrice) {
		t.Error("settings retrieval did not return default value")
	}
	if !settings.MinDownloadBandwidthPrice.Equals(skymodules.DefaultDownloadBandwidthPrice) {
		t.Error("settings retrieval did not return default value")
	}
	if !settings.MinStoragePrice.Equals(skymodules.DefaultStoragePrice) {
		t.Error("settings retrieval did not return default value")
	}
	if !settings.MinUploadBandwidthPrice.Equals(skymodules.DefaultUploadBandwidthPrice) {
		t.Error("settings retrieval did not return default value")
	}
	if settings.EphemeralAccountExpiry != (skymodules.DefaultEphemeralAccountExpiry) {
		t.Error("settings retrieval did not return default value")
	}
	if !settings.MaxEphemeralAccountBalance.Equals(skymodules.DefaultMaxEphemeralAccountBalance) {
		t.Error("settings retrieval did not return default value")
	}
	if !settings.MaxEphemeralAccountRisk.Equals(defaultMaxEphemeralAccountRisk) {
		t.Error("settings retrieval did not return default value")
	}

	// Check that calling SetInternalSettings with valid settings updates the settings.
	settings.AcceptingContracts = true
	settings.NetAddress = "foo.com:123"
	err = ht.host.SetInternalSettings(settings)
	if err != nil {
		t.Fatal(err)
	}
	settings = ht.host.InternalSettings()
	if settings.AcceptingContracts != true {
		t.Fatal("SetInternalSettings failed to update settings")
	}
	if settings.NetAddress != "foo.com:123" {
		t.Fatal("SetInternalSettings failed to update settings")
	}

	// Check that calling SetInternalSettings with invalid settings does not update the settings.
	settings.NetAddress = "invalid"
	err = ht.host.SetInternalSettings(settings)
	if err == nil {
		t.Fatal("expected SetInternalSettings to error with invalid settings")
	}
	settings = ht.host.InternalSettings()
	if settings.NetAddress != "foo.com:123" {
		t.Fatal("SetInternalSettings should not modify the settings if the new settings are invalid")
	}

	// Reload the host and verify that the altered settings persisted.
	err = ht.host.Close()
	if err != nil {
		t.Fatal(err)
	}
	rebootHost, err := New(ht.cs, ht.gateway, ht.tpool, ht.wallet, ht.mux, "localhost:0", filepath.Join(ht.persistDir, skymodules.HostDir))
	if err != nil {
		t.Fatal(err)
	}
	rebootSettings := rebootHost.InternalSettings()
	if rebootSettings.AcceptingContracts != settings.AcceptingContracts {
		t.Error("settings retrieval did not return updated value")
	}
	if rebootSettings.NetAddress != settings.NetAddress {
		t.Error("settings retrieval did not return updated value")
	}

	// Set ht.host to 'rebootHost' so that the 'ht.Close()' method will close
	// everything cleanly.
	ht.host = rebootHost
}

/*
// TestSetAndGetSettings checks that the functions for interacting with the
// hosts settings object are working as expected.
func TestSetAndGetSettings(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	ht, err := newHostTester("TestSetAndGetSettings")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
  if err := ht.Close(); err != nil {
t.Fatal(err)
}

}()

	// Check the default settings get returned at first call.
	settings := ht.host.Settings()
	if settings.MaxDuration != skymodules.DefaultMaxDuration {
		t.Error("settings retrieval did not return default value")
	}
	if settings.WindowSize != skymodules.DefaultWindowSize {
		t.Error("settings retrieval did not return default value")
	}
	if settings.Price.Cmp(defaultPrice) != 0 {
		t.Error("settings retrieval did not return default value")
	}
	if settings.Collateral.Cmp(skymodules.DefaultCollateral) != 0 {
		t.Error("settings retrieval did not return default value")
	}

	// Submit updated settings and check that the changes stuck.
	settings.TotalStorage += 15
	settings.MaxDuration += 16
	settings.WindowSize += 17
	settings.Price = settings.Price.Add(types.NewCurrency64(18))
	settings.Collateral = settings.Collateral.Add(types.NewCurrency64(19))
	err = ht.host.SetSettings(settings)
	if err != nil {
		t.Fatal(err)
	}
	newSettings := ht.host.Settings()
	if settings.MaxDuration != newSettings.MaxDuration {
		t.Error("settings retrieval did not return updated value")
	}
	if settings.WindowSize != newSettings.WindowSize {
		t.Error("settings retrieval did not return updated value")
	}
	if settings.Price.Cmp(newSettings.Price) != 0 {
		t.Error("settings retrieval did not return updated value")
	}
	if settings.Collateral.Cmp(newSettings.Collateral) != 0 {
		t.Error("settings retrieval did not return updated value")
	}

	// Reload the host and verify that the altered settings persisted.
	err = ht.host.Close()
	if err != nil {
		t.Fatal(err)
	}
	rebootHost, err := New(ht.cs, ht.tpool, ht.wallet, ht.mux, "localhost:0", filepath.Join(ht.persistDir, skymodules.HostDir))
	if err != nil {
		t.Fatal(err)
	}
	rebootSettings := rebootHost.Settings()
	if settings.TotalStorage != rebootSettings.TotalStorage {
		t.Error("settings retrieval did not return updated value")
	}
	if settings.MaxDuration != rebootSettings.MaxDuration {
		t.Error("settings retrieval did not return updated value")
	}
	if settings.WindowSize != rebootSettings.WindowSize {
		t.Error("settings retrieval did not return updated value")
	}
	if settings.Price.Cmp(rebootSettings.Price) != 0 {
		t.Error("settings retrieval did not return updated value")
	}
	if settings.Collateral.Cmp(rebootSettings.Collateral) != 0 {
		t.Error("settings retrieval did not return updated value")
	}
}

// TestPersistentSettings checks that settings persist between instances of the
// host.
func TestPersistentSettings(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	ht, err := newHostTester("TestSetPersistentSettings")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
  if err := ht.Close(); err != nil {
t.Fatal(err)
}

}()

	// Submit updated settings.
	settings := ht.host.Settings()
	settings.TotalStorage += 25
	settings.MaxDuration += 36
	settings.WindowSize += 47
	settings.Price = settings.Price.Add(types.NewCurrency64(38))
	settings.Collateral = settings.Collateral.Add(types.NewCurrency64(99))
	err = ht.host.SetSettings(settings)
	if err != nil {
		t.Fatal(err)
	}

	// Reboot the host and verify that the new settings stuck.
	err = ht.host.Close() // host saves upon closing
	if err != nil {
		t.Fatal(err)
	}
	h, err := New(ht.cs, ht.tpool, ht.wallet, ht.mux, "localhost:0", filepath.Join(ht.persistDir, skymodules.HostDir))
	if err != nil {
		t.Fatal(err)
	}
	newSettings := h.Settings()
	if settings.TotalStorage != newSettings.TotalStorage {
		t.Error("settings retrieval did not return updated value:", settings.TotalStorage, "vs", newSettings.TotalStorage)
	}
	if settings.MaxDuration != newSettings.MaxDuration {
		t.Error("settings retrieval did not return updated value")
	}
	if settings.WindowSize != newSettings.WindowSize {
		t.Error("settings retrieval did not return updated value")
	}
	if settings.Price.Cmp(newSettings.Price) != 0 {
		t.Error("settings retrieval did not return updated value")
	}
	if settings.Collateral.Cmp(newSettings.Collateral) != 0 {
		t.Error("settings retrieval did not return updated value")
	}
}
*/
