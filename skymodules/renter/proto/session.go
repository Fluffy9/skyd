package proto

import (
	"bytes"
	"crypto/cipher"
	"encoding/json"
	"fmt"
	"io"
	"math/bits"
	"net"
	"sort"
	"sync"
	"time"

	"gitlab.com/NebulousLabs/errors"
	"gitlab.com/NebulousLabs/log"
	"gitlab.com/NebulousLabs/ratelimit"

	"gitlab.com/NebulousLabs/Sia/crypto"
	"gitlab.com/NebulousLabs/Sia/types"
	"gitlab.com/skynetlabs/skyd/build"
	"gitlab.com/skynetlabs/skyd/skymodules"
)

// sessionDialTimeout determines how long a Session will try to dial a host
// before aborting.
var sessionDialTimeout = build.Select(build.Var{
	Testing:  5 * time.Second,
	Dev:      20 * time.Second,
	Standard: 45 * time.Second,
}).(time.Duration)

// A Session is an ongoing exchange of RPCs via the renter-host protocol.
//
// TODO: The session type needs access to a logger. Probably the renter logger.
type Session struct {
	aead        cipher.AEAD
	challenge   [16]byte
	closeChan   chan struct{}
	conn        net.Conn
	contractID  types.FileContractID
	contractSet *ContractSet
	deps        skymodules.Dependencies
	hdb         hostDB
	height      types.BlockHeight
	host        skymodules.HostDBEntry
	once        sync.Once
}

// writeRequest sends an encrypted RPC request to the host.
func (s *Session) writeRequest(rpcID types.Specifier, req interface{}) error {
	return skymodules.WriteRPCRequest(s.conn, s.aead, rpcID, req)
}

// writeResponse writes an encrypted RPC response to the host.
func (s *Session) writeResponse(resp interface{}, err error) error {
	return skymodules.WriteRPCResponse(s.conn, s.aead, resp, err)
}

// readResponse reads an encrypted RPC response from the host.
func (s *Session) readResponse(resp interface{}, maxLen uint64) error {
	return skymodules.ReadRPCResponse(s.conn, s.aead, resp, maxLen)
}

// call is a helper method that calls writeRequest followed by readResponse.
func (s *Session) call(rpcID types.Specifier, req, resp interface{}, maxLen uint64) error {
	if err := s.writeRequest(rpcID, req); err != nil {
		return err
	}
	return s.readResponse(resp, maxLen)
}

// Lock calls the Lock RPC, locking the supplied contract and returning its
// most recent revision.
func (s *Session) Lock(id types.FileContractID, secretKey crypto.SecretKey) (types.FileContractRevision, []types.TransactionSignature, error) {
	sig := crypto.SignHash(crypto.HashAll(skymodules.RPCChallengePrefix, s.challenge), secretKey)
	req := skymodules.LoopLockRequest{
		ContractID: id,
		Signature:  sig[:],
		Timeout:    defaultContractLockTimeout,
	}

	timeoutDur := time.Duration(defaultContractLockTimeout) * time.Millisecond
	extendDeadline(s.conn, skymodules.NegotiateSettingsTime+timeoutDur)
	var resp skymodules.LoopLockResponse
	if err := s.call(skymodules.RPCLoopLock, req, &resp, skymodules.RPCMinLen); err != nil {
		return types.FileContractRevision{}, nil, errors.AddContext(err, "lock request on host session has failed")
	}
	// Unconditionally update the challenge.
	s.challenge = resp.NewChallenge

	if !resp.Acquired {
		return resp.Revision, resp.Signatures, errors.New("contract is locked by another party")
	}
	// Set the new Session contract.
	s.contractID = id
	// Verify the public keys in the claimed revision.
	expectedUnlockConditions := types.UnlockConditions{
		PublicKeys: []types.SiaPublicKey{
			types.Ed25519PublicKey(secretKey.PublicKey()),
			s.host.PublicKey,
		},
		SignaturesRequired: 2,
	}
	if resp.Revision.UnlockConditions.UnlockHash() != expectedUnlockConditions.UnlockHash() {
		return resp.Revision, resp.Signatures, errors.New("host's claimed revision has wrong unlock conditions")
	}
	// Verify the claimed signatures.
	if err := skymodules.VerifyFileContractRevisionTransactionSignatures(resp.Revision, resp.Signatures, s.height); err != nil {
		return resp.Revision, resp.Signatures, errors.AddContext(err, "unable to verify signatures on contract revision")
	}
	return resp.Revision, resp.Signatures, nil
}

// Unlock calls the Unlock RPC, unlocking the currently-locked contract.
func (s *Session) Unlock() error {
	if s.contractID == (types.FileContractID{}) {
		return errors.New("no contract locked")
	}
	extendDeadline(s.conn, skymodules.NegotiateSettingsTime)
	return s.writeRequest(skymodules.RPCLoopUnlock, nil)
}

// HostSettings returns the currently active host settings of the session.
func (s *Session) HostSettings() skymodules.HostExternalSettings {
	return s.host.HostExternalSettings
}

// Settings calls the Settings RPC, returning the host's reported settings.
func (s *Session) Settings() (skymodules.HostExternalSettings, error) {
	extendDeadline(s.conn, skymodules.NegotiateSettingsTime)
	var resp skymodules.LoopSettingsResponse
	if err := s.call(skymodules.RPCLoopSettings, nil, &resp, skymodules.RPCMinLen); err != nil {
		return skymodules.HostExternalSettings{}, err
	}
	var hes skymodules.HostExternalSettings
	if err := json.Unmarshal(resp.Settings, &hes); err != nil {
		return skymodules.HostExternalSettings{}, err
	}
	s.host.HostExternalSettings = hes
	return s.host.HostExternalSettings, nil
}

// Append calls the Write RPC with a single Append action, returning the
// updated contract and the Merkle root of the appended sector.
func (s *Session) Append(data []byte) (_ skymodules.RenterContract, _ crypto.Hash, err error) {
	rc, err := s.Write([]skymodules.LoopWriteAction{{Type: skymodules.WriteActionAppend, Data: data}})
	return rc, crypto.MerkleRoot(data), err
}

// Replace calls the Write RPC with a series of actions that replace the sector
// at the specified index with data, returning the updated contract and the
// Merkle root of the new sector.
func (s *Session) Replace(data []byte, sectorIndex uint64, trim bool) (_ skymodules.RenterContract, _ crypto.Hash, err error) {
	sc, haveContract := s.contractSet.Acquire(s.contractID)
	if !haveContract {
		return skymodules.RenterContract{}, crypto.Hash{}, errors.New("contract not present in contract set")
	}
	defer s.contractSet.Return(sc)
	// get current number of sectors
	numSectors := sc.header.LastRevision().NewFileSize / skymodules.SectorSize
	actions := []skymodules.LoopWriteAction{
		// append the new sector
		{Type: skymodules.WriteActionAppend, Data: data},
		// swap the new sector with the old sector
		{Type: skymodules.WriteActionSwap, A: 0, B: numSectors},
	}
	if trim {
		// delete the old sector
		actions = append(actions, skymodules.LoopWriteAction{Type: skymodules.WriteActionTrim, A: 1})
	}

	rc, err := s.write(sc, actions)
	return rc, crypto.MerkleRoot(data), errors.AddContext(err, "write to host failed")
}

// Write implements the Write RPC, except for ActionUpdate. A Merkle proof is
// always requested.
func (s *Session) Write(actions []skymodules.LoopWriteAction) (_ skymodules.RenterContract, err error) {
	sc, haveContract := s.contractSet.Acquire(s.contractID)
	if !haveContract {
		return skymodules.RenterContract{}, errors.New("contract not present in contract set")
	}
	defer s.contractSet.Return(sc)
	return s.write(sc, actions)
}

func (s *Session) write(sc *SafeContract, actions []skymodules.LoopWriteAction) (_ skymodules.RenterContract, err error) {
	contract := sc.header // for convenience

	// calculate price per sector
	blockBytes := types.NewCurrency64(skymodules.SectorSize * uint64(contract.LastRevision().NewWindowEnd-s.height))
	sectorBandwidthPrice := s.host.UploadBandwidthPrice.Mul64(skymodules.SectorSize)
	sectorStoragePrice := s.host.StoragePrice.Mul(blockBytes)
	sectorCollateral := s.host.Collateral.Mul(blockBytes)

	// calculate the new Merkle root set and total cost/collateral
	var bandwidthPrice, storagePrice, collateral types.Currency
	newFileSize := contract.LastRevision().NewFileSize
	for _, action := range actions {
		switch action.Type {
		case skymodules.WriteActionAppend:
			bandwidthPrice = bandwidthPrice.Add(sectorBandwidthPrice)
			newFileSize += skymodules.SectorSize

		case skymodules.WriteActionTrim:
			newFileSize -= skymodules.SectorSize * action.A

		case skymodules.WriteActionSwap:

		case skymodules.WriteActionUpdate:
			return skymodules.RenterContract{}, errors.New("update not supported")

		default:
			build.Critical("unknown action type", action.Type)
		}
	}

	if newFileSize > contract.LastRevision().NewFileSize {
		addedSectors := (newFileSize - contract.LastRevision().NewFileSize) / skymodules.SectorSize
		storagePrice = sectorStoragePrice.Mul64(addedSectors)
		collateral = sectorCollateral.Mul64(addedSectors)
	}

	// estimate cost of Merkle proof
	proofSize := crypto.HashSize * (128 + len(actions))
	bandwidthPrice = bandwidthPrice.Add(s.host.DownloadBandwidthPrice.Mul64(uint64(proofSize)))

	// to mitigate small errors (e.g. differing block heights), fudge the
	// price and collateral by hostPriceLeeway.
	cost := s.host.BaseRPCPrice.Add(bandwidthPrice).Add(storagePrice).MulFloat(1 + hostPriceLeeway)
	collateral = collateral.MulFloat(1 - hostPriceLeeway)

	// check that enough funds are available
	if contract.RenterFunds().Cmp(cost) < 0 {
		return skymodules.RenterContract{}, errors.New("contract has insufficient funds to support upload")
	}
	if contract.LastRevision().MissedHostOutput().Value.Cmp(collateral) < 0 {
		// The contract doesn't have enough value in it to supply the
		// collateral. Instead of giving up, have the host put up everything
		// that remains, even if that is zero. The renter was aware when the
		// contract was formed that there may not be enough collateral in the
		// contract to cover the full storage needs for the renter, yet the
		// renter formed the contract anyway. Therefore the renter must think
		// that it's sufficient.
		//
		// TODO: log.Debugln here to indicate that the host is having issues
		// supplying collateral.
		//
		// TODO: We may in the future want to have the renter perform a renewal
		// on this contract so that the host can refill the collateral. That's a
		// concern at the renter level though, not at the session level. The
		// session should still be assuming that if the renter is willing to use
		// this contract, the renter is aware that there isn't enough collateral
		// remaining and is happy to use the contract anyway. Therefore this
		// TODO should be moved to a different part of the codebase.
		collateral = contract.LastRevision().MissedHostOutput().Value
	}

	// create the revision; we will update the Merkle root later
	rev, err := contract.LastRevision().PaymentRevision(cost)
	if err != nil {
		return skymodules.RenterContract{}, errors.AddContext(err, "Error creating new write revision")
	}

	rev.SetMissedHostPayout(rev.MissedHostOutput().Value.Sub(collateral))
	voidOutput, err := rev.MissedVoidOutput()
	rev.SetMissedVoidPayout(voidOutput.Value.Add(collateral))
	rev.NewFileSize = newFileSize

	// create the request
	req := skymodules.LoopWriteRequest{
		Actions:           actions,
		MerkleProof:       true,
		NewRevisionNumber: rev.NewRevisionNumber,
	}
	req.NewValidProofValues = make([]types.Currency, len(rev.NewValidProofOutputs))
	for i, o := range rev.NewValidProofOutputs {
		req.NewValidProofValues[i] = o.Value
	}
	req.NewMissedProofValues = make([]types.Currency, len(rev.NewMissedProofOutputs))
	for i, o := range rev.NewMissedProofOutputs {
		req.NewMissedProofValues[i] = o.Value
	}

	// record the change we are about to make to the contract. If we lose power
	// mid-revision, this allows us to restore either the pre-revision or
	// post-revision contract.
	//
	// TODO: update this for non-local root storage
	walTxn, err := sc.managedRecordAppendIntent(rev, crypto.Hash{}, storagePrice, bandwidthPrice)
	if err != nil {
		return skymodules.RenterContract{}, err
	}

	defer func() {
		// Increase Successful/Failed interactions accordingly
		if err != nil {
			s.hdb.IncrementFailedInteractions(s.host.PublicKey)
		} else {
			s.hdb.IncrementSuccessfulInteractions(s.host.PublicKey)
		}

		// reset deadline
		extendDeadline(s.conn, time.Hour)
	}()

	// Disrupt here before sending the signed revision to the host.
	if s.deps.Disrupt("InterruptUploadBeforeSendingRevision") {
		return skymodules.RenterContract{}, errors.New("InterruptUploadBeforeSendingRevision disrupt")
	}

	// send Write RPC request
	extendDeadline(s.conn, skymodules.NegotiateFileContractRevisionTime)
	if err := s.writeRequest(skymodules.RPCLoopWrite, req); err != nil {
		return skymodules.RenterContract{}, err
	}

	// read Merkle proof from host
	var merkleResp skymodules.LoopWriteMerkleProof
	if err := s.readResponse(&merkleResp, skymodules.RPCMinLen); err != nil {
		return skymodules.RenterContract{}, err
	}
	// verify the proof, first by verifying the old Merkle root...
	numSectors := contract.LastRevision().NewFileSize / skymodules.SectorSize
	proofRanges := calculateProofRanges(actions, numSectors)
	proofHashes := merkleResp.OldSubtreeHashes
	leafHashes := merkleResp.OldLeafHashes
	oldRoot, newRoot := contract.LastRevision().NewFileMerkleRoot, merkleResp.NewMerkleRoot
	if !crypto.VerifyDiffProof(proofRanges, numSectors, proofHashes, leafHashes, oldRoot) {
		return skymodules.RenterContract{}, errors.New("invalid Merkle proof for old root")
	}
	// ...then by modifying the leaves and verifying the new Merkle root
	leafHashes = modifyLeaves(leafHashes, actions, numSectors)
	proofRanges = modifyProofRanges(proofRanges, actions, numSectors)
	if !crypto.VerifyDiffProof(proofRanges, numSectors, proofHashes, leafHashes, newRoot) {
		return skymodules.RenterContract{}, errors.New("invalid Merkle proof for new root")
	}

	// update the revision, sign it, and send it
	rev.NewFileMerkleRoot = newRoot
	txn := types.Transaction{
		FileContractRevisions: []types.FileContractRevision{rev},
		TransactionSignatures: []types.TransactionSignature{
			{
				ParentID:       crypto.Hash(rev.ParentID),
				CoveredFields:  types.CoveredFields{FileContractRevisions: []uint64{0}},
				PublicKeyIndex: 0, // renter key is always first -- see formContract
			},
			{
				ParentID:       crypto.Hash(rev.ParentID),
				PublicKeyIndex: 1,
				CoveredFields:  types.CoveredFields{FileContractRevisions: []uint64{0}},
				Signature:      nil, // to be provided by host
			},
		},
	}
	sig := crypto.SignHash(txn.SigHash(0, s.height), contract.SecretKey)
	txn.TransactionSignatures[0].Signature = sig[:]
	renterSig := skymodules.LoopWriteResponse{
		Signature: sig[:],
	}
	if err := s.writeResponse(renterSig, nil); err != nil {
		return skymodules.RenterContract{}, err
	}

	// read the host's signature
	var hostSig skymodules.LoopWriteResponse
	if err := s.readResponse(&hostSig, skymodules.RPCMinLen); err != nil {
		// If the host was OOS, we update the contract utility.
		if skymodules.IsOOSErr(err) {
			u := sc.Utility()
			u.GoodForUpload = false // Stop uploading to such a host immediately.
			u.LastOOSErr = s.height
			err = errors.Compose(err, sc.UpdateUtility(u))
		}
		return skymodules.RenterContract{}, errors.AddContext(err, "marking host as not good for upload because the host is out of storage")
	}
	txn.TransactionSignatures[1].Signature = hostSig.Signature

	// Disrupt here before updating the contract.
	if s.deps.Disrupt("InterruptUploadAfterSendingRevision") {
		return skymodules.RenterContract{}, errors.New("InterruptUploadAfterSendingRevision disrupt")
	}

	// update contract
	//
	// TODO: unnecessary?
	err = sc.managedCommitAppend(walTxn, txn, storagePrice, bandwidthPrice)
	if err != nil {
		return skymodules.RenterContract{}, err
	}
	return sc.Metadata(), nil
}

// Read calls the Read RPC, writing the requested data to w. The RPC can be
// cancelled (with a granularity of one section) via the cancel channel.
func (s *Session) Read(w io.Writer, req skymodules.LoopReadRequest, cancel <-chan struct{}) (_ skymodules.RenterContract, err error) {
	// Reset deadline when finished.
	defer extendDeadline(s.conn, time.Hour)

	// Sanity-check the request.
	for _, sec := range req.Sections {
		if uint64(sec.Offset)+uint64(sec.Length) > skymodules.SectorSize {
			return skymodules.RenterContract{}, errors.New("illegal offset and/or length")
		}
		if req.MerkleProof {
			if sec.Offset%crypto.SegmentSize != 0 || sec.Length%crypto.SegmentSize != 0 {
				return skymodules.RenterContract{}, errors.New("offset and length must be multiples of SegmentSize when requesting a Merkle proof")
			}
		}
	}

	// Acquire the contract.
	sc, haveContract := s.contractSet.Acquire(s.contractID)
	if !haveContract {
		return skymodules.RenterContract{}, errors.New("contract not present in contract set")
	}
	defer s.contractSet.Return(sc)
	contract := sc.header // for convenience

	// calculate estimated bandwidth
	var totalLength uint64
	for _, sec := range req.Sections {
		totalLength += uint64(sec.Length)
	}
	var estProofHashes uint64
	if req.MerkleProof {
		// use the worst-case proof size of 2*tree depth (this occurs when
		// proving across the two leaves in the center of the tree)
		estHashesPerProof := 2 * bits.Len64(skymodules.SectorSize/crypto.SegmentSize)
		estProofHashes = uint64(len(req.Sections) * estHashesPerProof)
	}
	estBandwidth := totalLength + estProofHashes*crypto.HashSize
	if estBandwidth < skymodules.RPCMinLen {
		estBandwidth = skymodules.RPCMinLen
	}
	// calculate sector accesses
	sectorAccesses := make(map[crypto.Hash]struct{})
	for _, sec := range req.Sections {
		sectorAccesses[sec.MerkleRoot] = struct{}{}
	}
	// calculate price
	bandwidthPrice := s.host.DownloadBandwidthPrice.Mul64(estBandwidth)
	sectorAccessPrice := s.host.SectorAccessPrice.Mul64(uint64(len(sectorAccesses)))
	price := s.host.BaseRPCPrice.Add(bandwidthPrice).Add(sectorAccessPrice)
	if contract.RenterFunds().Cmp(price) < 0 {
		return skymodules.RenterContract{}, errors.New("contract has insufficient funds to support download")
	}
	// To mitigate small errors (e.g. differing block heights), fudge the
	// price and collateral by 0.2%.
	price = price.MulFloat(1 + hostPriceLeeway)

	// create the download revision and sign it
	rev, err := newDownloadRevision(contract.LastRevision(), price)
	if err != nil {
		return skymodules.RenterContract{}, errors.AddContext(err, "Error creating new download revision")
	}

	txn := types.Transaction{
		FileContractRevisions: []types.FileContractRevision{rev},
		TransactionSignatures: []types.TransactionSignature{
			{
				ParentID:       crypto.Hash(rev.ParentID),
				CoveredFields:  types.CoveredFields{FileContractRevisions: []uint64{0}},
				PublicKeyIndex: 0, // renter key is always first -- see formContract
			},
			{
				ParentID:       crypto.Hash(rev.ParentID),
				PublicKeyIndex: 1,
				CoveredFields:  types.CoveredFields{FileContractRevisions: []uint64{0}},
				Signature:      nil, // to be provided by host
			},
		},
	}
	sig := crypto.SignHash(txn.SigHash(0, s.height), contract.SecretKey)
	txn.TransactionSignatures[0].Signature = sig[:]

	req.NewRevisionNumber = rev.NewRevisionNumber
	req.NewValidProofValues = make([]types.Currency, len(rev.NewValidProofOutputs))
	for i, o := range rev.NewValidProofOutputs {
		req.NewValidProofValues[i] = o.Value
	}
	req.NewMissedProofValues = make([]types.Currency, len(rev.NewMissedProofOutputs))
	for i, o := range rev.NewMissedProofOutputs {
		req.NewMissedProofValues[i] = o.Value
	}
	req.Signature = sig[:]

	// record the change we are about to make to the contract. If we lose power
	// mid-revision, this allows us to restore either the pre-revision or
	// post-revision contract.
	walTxn, err := sc.managedRecordDownloadIntent(rev, price)
	if err != nil {
		return skymodules.RenterContract{}, err
	}

	// Increase Successful/Failed interactions accordingly
	defer func() {
		if err != nil {
			s.hdb.IncrementFailedInteractions(contract.HostPublicKey())
		} else {
			s.hdb.IncrementSuccessfulInteractions(contract.HostPublicKey())
		}
	}()

	// Disrupt before sending the signed revision to the host.
	if s.deps.Disrupt("InterruptDownloadBeforeSendingRevision") {
		return skymodules.RenterContract{}, errors.New("InterruptDownloadBeforeSendingRevision disrupt")
	}

	// send request
	extendDeadline(s.conn, skymodules.NegotiateDownloadTime)
	err = s.writeRequest(skymodules.RPCLoopRead, req)
	if err != nil {
		return skymodules.RenterContract{}, err
	}

	// spawn a goroutine to handle cancellation
	doneChan := make(chan struct{})
	go func() {
		select {
		case <-cancel:
		case <-doneChan:
		}
		s.writeResponse(skymodules.RPCLoopReadStop, nil)
	}()
	// ensure we send RPCLoopReadStop before returning
	defer close(doneChan)

	// read responses
	var hostSig []byte
	for _, sec := range req.Sections {
		var resp skymodules.LoopReadResponse
		err = s.readResponse(&resp, skymodules.RPCMinLen+uint64(sec.Length))
		if err != nil {
			return skymodules.RenterContract{}, err
		}
		// The host may have sent data, a signature, or both. If they sent data,
		// validate it.
		if len(resp.Data) > 0 {
			if len(resp.Data) != int(sec.Length) {
				return skymodules.RenterContract{}, errors.New("host did not send enough sector data")
			}
			if req.MerkleProof {
				proofStart := int(sec.Offset) / crypto.SegmentSize
				proofEnd := int(sec.Offset+sec.Length) / crypto.SegmentSize
				if !crypto.VerifyRangeProof(resp.Data, resp.MerkleProof, proofStart, proofEnd, sec.MerkleRoot) {
					return skymodules.RenterContract{}, errors.New("host provided incorrect sector data or Merkle proof")
				}
			}
			// write sector data
			if _, err := w.Write(resp.Data); err != nil {
				return skymodules.RenterContract{}, err
			}
		}
		// If the host sent a signature, exit the loop; they won't be sending
		// any more data
		if len(resp.Signature) > 0 {
			hostSig = resp.Signature
			break
		}
	}
	if hostSig == nil {
		// the host is required to send a signature; if they haven't sent one
		// yet, they should send an empty ReadResponse containing just the
		// signature.
		var resp skymodules.LoopReadResponse
		err = s.readResponse(&resp, skymodules.RPCMinLen)
		if err != nil {
			return skymodules.RenterContract{}, err
		}
		hostSig = resp.Signature
	}
	txn.TransactionSignatures[1].Signature = hostSig

	// Disrupt before committing.
	if s.deps.Disrupt("InterruptDownloadAfterSendingRevision") {
		return skymodules.RenterContract{}, errors.New("InterruptDownloadAfterSendingRevision disrupt")
	}

	// update contract and metrics
	if err := sc.managedCommitDownload(walTxn, txn, price); err != nil {
		return skymodules.RenterContract{}, err
	}

	return sc.Metadata(), nil
}

// ReadSection calls the Read RPC with a single section and returns the
// requested data. A Merkle proof is always requested.
func (s *Session) ReadSection(root crypto.Hash, offset, length uint32) (_ skymodules.RenterContract, _ []byte, err error) {
	req := skymodules.LoopReadRequest{
		Sections: []skymodules.LoopReadRequestSection{{
			MerkleRoot: root,
			Offset:     offset,
			Length:     length,
		}},
		MerkleProof: true,
	}
	var buf bytes.Buffer
	buf.Grow(int(length))
	contract, err := s.Read(&buf, req, nil)
	return contract, buf.Bytes(), err
}

// SectorRoots calls the contract roots download RPC and returns the requested sector roots. The
// Revision and Signature fields of req are filled in automatically. If a
// Merkle proof is requested, it is verified.
func (s *Session) SectorRoots(req skymodules.LoopSectorRootsRequest) (_ skymodules.RenterContract, _ []crypto.Hash, err error) {
	// Reset deadline when finished.
	defer extendDeadline(s.conn, time.Hour)

	// Acquire the contract.
	sc, haveContract := s.contractSet.Acquire(s.contractID)
	if !haveContract {
		return skymodules.RenterContract{}, nil, errors.New("contract not present in contract set")
	}
	defer s.contractSet.Return(sc)
	contract := sc.header // for convenience

	// calculate price
	estProofHashes := bits.Len64(contract.LastRevision().NewFileSize / skymodules.SectorSize)
	estBandwidth := (uint64(estProofHashes) + req.NumRoots) * crypto.HashSize
	if estBandwidth < skymodules.RPCMinLen {
		estBandwidth = skymodules.RPCMinLen
	}
	bandwidthPrice := s.host.DownloadBandwidthPrice.Mul64(estBandwidth)
	price := s.host.BaseRPCPrice.Add(bandwidthPrice)
	if contract.RenterFunds().Cmp(price) < 0 {
		return skymodules.RenterContract{}, nil, errors.New("contract has insufficient funds to support sector roots download")
	}
	// To mitigate small errors (e.g. differing block heights), fudge the
	// price and collateral by 0.2%.
	price = price.MulFloat(1 + hostPriceLeeway)

	// create the download revision and sign it
	rev, err := newDownloadRevision(contract.LastRevision(), price)
	if err != nil {
		return skymodules.RenterContract{}, nil, errors.AddContext(err, "Error creating new download revision")
	}

	txn := types.Transaction{
		FileContractRevisions: []types.FileContractRevision{rev},
		TransactionSignatures: []types.TransactionSignature{
			{
				ParentID:       crypto.Hash(rev.ParentID),
				CoveredFields:  types.CoveredFields{FileContractRevisions: []uint64{0}},
				PublicKeyIndex: 0, // renter key is always first -- see formContract
			},
			{
				ParentID:       crypto.Hash(rev.ParentID),
				PublicKeyIndex: 1,
				CoveredFields:  types.CoveredFields{FileContractRevisions: []uint64{0}},
				Signature:      nil, // to be provided by host
			},
		},
	}
	sig := crypto.SignHash(txn.SigHash(0, s.height), contract.SecretKey)
	txn.TransactionSignatures[0].Signature = sig[:]

	// fill in the missing request fields
	req.NewRevisionNumber = rev.NewRevisionNumber
	req.NewValidProofValues = make([]types.Currency, len(rev.NewValidProofOutputs))
	for i, o := range rev.NewValidProofOutputs {
		req.NewValidProofValues[i] = o.Value
	}
	req.NewMissedProofValues = make([]types.Currency, len(rev.NewMissedProofOutputs))
	for i, o := range rev.NewMissedProofOutputs {
		req.NewMissedProofValues[i] = o.Value
	}
	req.Signature = sig[:]

	// record the change we are about to make to the contract. If we lose power
	// mid-revision, this allows us to restore either the pre-revision or
	// post-revision contract.
	walTxn, err := sc.managedRecordDownloadIntent(rev, price)
	if err != nil {
		return skymodules.RenterContract{}, nil, err
	}

	// Increase Successful/Failed interactions accordingly
	defer func() {
		if err != nil {
			s.hdb.IncrementFailedInteractions(contract.HostPublicKey())
		} else {
			s.hdb.IncrementSuccessfulInteractions(contract.HostPublicKey())
		}
	}()

	// send SectorRoots RPC request
	extendDeadline(s.conn, skymodules.NegotiateDownloadTime)
	var resp skymodules.LoopSectorRootsResponse
	err = s.call(skymodules.RPCLoopSectorRoots, req, &resp, skymodules.RPCMinLen+(req.NumRoots*crypto.HashSize))
	if err != nil {
		return skymodules.RenterContract{}, nil, err
	}
	// verify the response
	if len(resp.SectorRoots) != int(req.NumRoots) {
		return skymodules.RenterContract{}, nil, errors.New("host did not send the requested number of sector roots")
	}
	proofStart, proofEnd := int(req.RootOffset), int(req.RootOffset+req.NumRoots)
	if !crypto.VerifySectorRangeProof(resp.SectorRoots, resp.MerkleProof, proofStart, proofEnd, rev.NewFileMerkleRoot) {
		return skymodules.RenterContract{}, nil, errors.New("host provided incorrect sector data or Merkle proof")
	}

	// add host signature
	txn.TransactionSignatures[1].Signature = resp.Signature

	// update contract and metrics
	if err := sc.managedCommitDownload(walTxn, txn, price); err != nil {
		return skymodules.RenterContract{}, nil, err
	}

	return sc.Metadata(), resp.SectorRoots, nil
}

// RecoverSectorRoots calls the contract roots download RPC and returns the requested sector roots. The
// Revision and Signature fields of req are filled in automatically. If a
// Merkle proof is requested, it is verified.
func (s *Session) RecoverSectorRoots(lastRev types.FileContractRevision, sk crypto.SecretKey) (_ types.Transaction, _ []crypto.Hash, err error) {
	// Calculate total roots we need to fetch.
	numRoots := lastRev.NewFileSize / skymodules.SectorSize
	if lastRev.NewFileSize%skymodules.SectorSize != 0 {
		numRoots++
	}
	// Create the request.
	req := skymodules.LoopSectorRootsRequest{
		RootOffset: 0,
		NumRoots:   numRoots,
	}
	// Reset deadline when finished.
	defer extendDeadline(s.conn, time.Hour)

	// calculate price
	estProofHashes := bits.Len64(lastRev.NewFileSize / skymodules.SectorSize)
	estBandwidth := (uint64(estProofHashes) + req.NumRoots) * crypto.HashSize
	if estBandwidth < skymodules.RPCMinLen {
		estBandwidth = skymodules.RPCMinLen
	}
	bandwidthPrice := s.host.DownloadBandwidthPrice.Mul64(estBandwidth)
	price := s.host.BaseRPCPrice.Add(bandwidthPrice)
	if lastRev.ValidRenterPayout().Cmp(price) < 0 {
		return types.Transaction{}, nil, errors.New("contract has insufficient funds to support sector roots download")
	}
	// To mitigate small errors (e.g. differing block heights), fudge the
	// price and collateral by 0.2%.
	price = price.MulFloat(1 + hostPriceLeeway)

	// create the download revision and sign it
	rev, err := newDownloadRevision(lastRev, price)
	if err != nil {
		return types.Transaction{}, nil, errors.AddContext(err, "Error creating new download revision")
	}

	txn := types.Transaction{
		FileContractRevisions: []types.FileContractRevision{rev},
		TransactionSignatures: []types.TransactionSignature{
			{
				ParentID:       crypto.Hash(rev.ParentID),
				CoveredFields:  types.CoveredFields{FileContractRevisions: []uint64{0}},
				PublicKeyIndex: 0, // renter key is always first -- see formContract
			},
			{
				ParentID:       crypto.Hash(rev.ParentID),
				PublicKeyIndex: 1,
				CoveredFields:  types.CoveredFields{FileContractRevisions: []uint64{0}},
				Signature:      nil, // to be provided by host
			},
		},
	}
	sig := crypto.SignHash(txn.SigHash(0, s.height), sk)
	txn.TransactionSignatures[0].Signature = sig[:]

	// fill in the missing request fields
	req.NewRevisionNumber = rev.NewRevisionNumber
	req.NewValidProofValues = make([]types.Currency, len(rev.NewValidProofOutputs))
	for i, o := range rev.NewValidProofOutputs {
		req.NewValidProofValues[i] = o.Value
	}
	req.NewMissedProofValues = make([]types.Currency, len(rev.NewMissedProofOutputs))
	for i, o := range rev.NewMissedProofOutputs {
		req.NewMissedProofValues[i] = o.Value
	}
	req.Signature = sig[:]

	// Increase Successful/Failed interactions accordingly
	defer func() {
		if err != nil {
			s.hdb.IncrementFailedInteractions(s.host.PublicKey)
		} else {
			s.hdb.IncrementSuccessfulInteractions(s.host.PublicKey)
		}
	}()

	// send SectorRoots RPC request
	extendDeadline(s.conn, skymodules.NegotiateDownloadTime)
	var resp skymodules.LoopSectorRootsResponse
	err = s.call(skymodules.RPCLoopSectorRoots, req, &resp, skymodules.RPCMinLen+(req.NumRoots*crypto.HashSize))
	if err != nil {
		return types.Transaction{}, nil, err
	}
	// verify the response
	if len(resp.SectorRoots) != int(req.NumRoots) {
		return types.Transaction{}, nil, errors.New("host did not send the requested number of sector roots")
	}
	proofStart, proofEnd := int(req.RootOffset), int(req.RootOffset+req.NumRoots)
	if !crypto.VerifySectorRangeProof(resp.SectorRoots, resp.MerkleProof, proofStart, proofEnd, rev.NewFileMerkleRoot) {
		return types.Transaction{}, nil, errors.New("host provided incorrect sector data or Merkle proof")
	}

	// add host signature
	txn.TransactionSignatures[1].Signature = resp.Signature
	return txn, resp.SectorRoots, nil
}

// shutdown terminates the revision loop and signals the goroutine spawned in
// NewSession to return.
func (s *Session) shutdown() {
	extendDeadline(s.conn, skymodules.NegotiateSettingsTime)
	// don't care about this error
	_ = s.writeRequest(skymodules.RPCLoopExit, nil)
	close(s.closeChan)
}

// Close cleanly terminates the protocol session with the host and closes the
// connection.
func (s *Session) Close() error {
	// using once ensures that Close is idempotent
	s.once.Do(s.shutdown)
	return s.conn.Close()
}

// NewSession initiates the RPC loop with a host and returns a Session.
func (cs *ContractSet) NewSession(host skymodules.HostDBEntry, id types.FileContractID, currentHeight types.BlockHeight, hdb hostDB, logger *log.Logger, cancel <-chan struct{}) (_ *Session, err error) {
	sc, ok := cs.Acquire(id)
	if !ok {
		return nil, errors.New("could not locate contract to create session")
	}
	defer cs.Return(sc)
	s, err := cs.managedNewSession(host, currentHeight, hdb, cancel)
	if err != nil {
		return nil, errors.AddContext(err, "unable to create a new session with the host")
	}
	// Lock the contract
	rev, sigs, err := s.Lock(id, sc.header.SecretKey)
	if err != nil {
		s.Close()
		return nil, errors.AddContext(err, "unable to get a session lock")
	}

	// Resynchronize
	err = sc.managedSyncRevision(rev, sigs)
	if err != nil {
		logger.Printf("%v revision resync failed, err: %v\n", host.PublicKey.String(), err)
		err = errors.Compose(err, s.Close())
		return nil, errors.AddContext(err, "unable to sync revisions when creating session")
	}
	logger.Debugf("%v revision resync attempted, succeeded: %v\n", host.PublicKey.String(), sc.LastRevision().NewRevisionNumber == rev.NewRevisionNumber)

	return s, nil
}

// NewRawSession creates a new session unassociated with any contract.
func (cs *ContractSet) NewRawSession(host skymodules.HostDBEntry, currentHeight types.BlockHeight, hdb hostDB, cancel <-chan struct{}) (_ *Session, err error) {
	return cs.managedNewSession(host, currentHeight, hdb, cancel)
}

// managedNewSession initiates the RPC loop with a host and returns a Session.
func (cs *ContractSet) managedNewSession(host skymodules.HostDBEntry, currentHeight types.BlockHeight, hdb hostDB, cancel <-chan struct{}) (_ *Session, err error) {
	// Increase Successful/Failed interactions accordingly
	defer func() {
		if err != nil {
			hdb.IncrementFailedInteractions(host.PublicKey)
			err = errors.Extend(err, skymodules.ErrHostFault)
		} else {
			hdb.IncrementSuccessfulInteractions(host.PublicKey)
		}
	}()

	// If we are using a custom resolver we need to replace the domain name
	// with 127.0.0.1 to be able to dial the host.
	if cs.staticDeps.Disrupt("customResolver") {
		port := host.NetAddress.Port()
		host.NetAddress = skymodules.NetAddress(fmt.Sprintf("127.0.0.1:%s", port))
	}

	c, err := (&net.Dialer{
		Cancel:  cancel,
		Timeout: sessionDialTimeout,
	}).Dial("tcp", string(host.NetAddress))
	if err != nil {
		return nil, errors.AddContext(err, "unsuccessful dial when creating a new session")
	}
	conn := ratelimit.NewRLConn(c, cs.staticRL, cancel)

	closeChan := make(chan struct{})
	go func() {
		select {
		case <-cancel:
			conn.Close()
		case <-closeChan:
			// we don't close the connection here because we want session.Close
			// to be able to return the Close error directly
		}
	}()

	// Perform the handshake and create the session object.
	aead, challenge, err := performSessionHandshake(conn, host.PublicKey)
	if err != nil {
		conn.Close()
		close(closeChan)
		return nil, errors.AddContext(err, "session handshake failed")
	}
	s := &Session{
		aead:        aead,
		challenge:   challenge.Challenge,
		closeChan:   closeChan,
		conn:        conn,
		contractSet: cs,
		deps:        cs.staticDeps,
		hdb:         hdb,
		height:      currentHeight,
		host:        host,
	}

	return s, nil
}

// calculateProofRanges returns the proof ranges that should be used to verify a
// pre-modification Merkle diff proof for the specified actions.
func calculateProofRanges(actions []skymodules.LoopWriteAction, oldNumSectors uint64) []crypto.ProofRange {
	newNumSectors := oldNumSectors
	sectorsChanged := make(map[uint64]struct{})
	for _, action := range actions {
		switch action.Type {
		case skymodules.WriteActionAppend:
			sectorsChanged[newNumSectors] = struct{}{}
			newNumSectors++

		case skymodules.WriteActionTrim:
			newNumSectors--
			sectorsChanged[newNumSectors] = struct{}{}

		case skymodules.WriteActionSwap:
			sectorsChanged[action.A] = struct{}{}
			sectorsChanged[action.B] = struct{}{}

		case skymodules.WriteActionUpdate:
			panic("update not supported")
		}
	}

	oldRanges := make([]crypto.ProofRange, 0, len(sectorsChanged))
	for index := range sectorsChanged {
		if index < oldNumSectors {
			oldRanges = append(oldRanges, crypto.ProofRange{
				Start: index,
				End:   index + 1,
			})
		}
	}
	sort.Slice(oldRanges, func(i, j int) bool {
		return oldRanges[i].Start < oldRanges[j].Start
	})

	return oldRanges
}

// modifyProofRanges modifies the proof ranges produced by calculateProofRanges
// to verify a post-modification Merkle diff proof for the specified actions.
func modifyProofRanges(proofRanges []crypto.ProofRange, actions []skymodules.LoopWriteAction, numSectors uint64) []crypto.ProofRange {
	for _, action := range actions {
		switch action.Type {
		case skymodules.WriteActionAppend:
			proofRanges = append(proofRanges, crypto.ProofRange{
				Start: numSectors,
				End:   numSectors + 1,
			})
			numSectors++

		case skymodules.WriteActionTrim:
			proofRanges = proofRanges[:uint64(len(proofRanges))-action.A]
			numSectors--
		}
	}
	return proofRanges
}

// modifyLeaves modifies the leaf hashes of a Merkle diff proof to verify a
// post-modification Merkle diff proof for the specified actions.
func modifyLeaves(leafHashes []crypto.Hash, actions []skymodules.LoopWriteAction, numSectors uint64) []crypto.Hash {
	// determine which sector index corresponds to each leaf hash
	var indices []uint64
	for _, action := range actions {
		switch action.Type {
		case skymodules.WriteActionAppend:
			indices = append(indices, numSectors)
			numSectors++
		case skymodules.WriteActionTrim:
			for j := uint64(0); j < action.A; j++ {
				indices = append(indices, numSectors)
				numSectors--
			}
		case skymodules.WriteActionSwap:
			indices = append(indices, action.A, action.B)
		}
	}
	sort.Slice(indices, func(i, j int) bool {
		return indices[i] < indices[j]
	})
	indexMap := make(map[uint64]int, len(leafHashes))
	for i, index := range indices {
		if i > 0 && index == indices[i-1] {
			continue // remove duplicates
		}
		indexMap[index] = i
	}

	for _, action := range actions {
		switch action.Type {
		case skymodules.WriteActionAppend:
			leafHashes = append(leafHashes, crypto.MerkleRoot(action.Data))

		case skymodules.WriteActionTrim:
			leafHashes = leafHashes[:uint64(len(leafHashes))-action.A]

		case skymodules.WriteActionSwap:
			i, j := indexMap[action.A], indexMap[action.B]
			leafHashes[i], leafHashes[j] = leafHashes[j], leafHashes[i]

		case skymodules.WriteActionUpdate:
			panic("update not supported")
		}
	}
	return leafHashes
}