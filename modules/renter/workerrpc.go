package renter

import (
	"encoding/json"
	"fmt"
	"io"

	"gitlab.com/NebulousLabs/Sia/build"
	"gitlab.com/NebulousLabs/Sia/crypto"
	"gitlab.com/NebulousLabs/Sia/modules"
	"gitlab.com/NebulousLabs/Sia/types"
	"gitlab.com/NebulousLabs/errors"
	"gitlab.com/NebulousLabs/siamux"
)

// programResponse is a helper struct that wraps the RPCExecuteProgramResponse
// alongside the data output
type programResponse struct {
	modules.RPCExecuteProgramResponse
	Output []byte
}

// managedExecuteProgram performs the ExecuteProgramRPC on the host
func (w *worker) managedExecuteProgram(p modules.Program, data []byte, fcid types.FileContractID, cost types.Currency) ([]programResponse, error) {
	// check host version
	if build.VersionCmp(w.staticHostVersion, compatRHProtocolVersion) < 0 {
		build.Critical("Executing new RHP RPC on host with version", w.staticHostVersion)
	}

	// create a new stream
	stream, err := w.staticNewStream()
	if err != nil {
		return nil, errors.AddContext(err, "Unable to create a new stream")
	}
	defer func() {
		if err := stream.Close(); err != nil {
			w.renter.log.Println("ERROR: failed to close stream", err)
		}
	}()

	// grab some variables from the worker
	w.mu.Lock()
	bh := w.cachedBlockHeight
	w.mu.Unlock()

	// write the specifier
	err = modules.RPCWrite(stream, modules.RPCExecuteProgram)
	if err != nil {
		return nil, err
	}

	// send price table uid
	pt := w.staticHostPrices.managedPriceTable()
	err = modules.RPCWrite(stream, pt.UID)
	if err != nil {
		return nil, err
	}

	// prepare the request.
	epr := modules.RPCExecuteProgramRequest{
		FileContractID:    fcid,
		Program:           p,
		ProgramDataLength: uint64(len(data)),
	}

	// provide payment
	err = w.staticAccount.ProvidePayment(stream, w.staticHostPubKey, modules.RPCUpdatePriceTable, cost, w.staticAccount.staticID, bh)
	if err != nil {
		return nil, err
	}

	// send the execute program request.
	err = modules.RPCWrite(stream, epr)
	if err != nil {
		return nil, err
	}

	// send the programData.
	_, err = stream.Write(data)
	if err != nil {
		return nil, err
	}

	// read the responses.
	responses := make([]programResponse, len(epr.Program))
	for i := range responses {
		err = modules.RPCRead(stream, &responses[i])
		if err != nil {
			return nil, err
		}

		// Read the output data.
		outputLen := responses[i].OutputLength
		responses[i].Output = make([]byte, outputLen, outputLen)
		_, err = io.ReadFull(stream, responses[i].Output)
		if err != nil {
			return nil, err
		}

		// If the response contains an error we are done.
		if responses[i].Error != nil {
			break
		}
	}
	return responses, nil
}

// managedFundAccount will call the fundAccountRPC on the host and if successful
// will deposit the given amount into the worker's ephemeral account.
func (w *worker) managedFundAccount(amount types.Currency) (modules.FundAccountResponse, error) {
	// check host version
	if build.VersionCmp(w.staticHostVersion, compatRHProtocolVersion) < 0 {
		build.Critical("Executing new RHP RPC on host with version", w.staticHostVersion)
	}

	// create a new stream
	stream, err := w.staticNewStream()
	if err != nil {
		return modules.FundAccountResponse{}, errors.AddContext(err, "Unable to create a new stream")
	}
	defer func() {
		if err := stream.Close(); err != nil {
			w.renter.log.Println("ERROR: failed to close stream", err)
		}
	}()

	// grab some variables from the worker
	w.mu.Lock()
	bh := w.cachedBlockHeight
	w.mu.Unlock()

	// close the stream
	defer func() {
		if err := stream.Close(); err != nil {
			w.renter.log.Println("ERROR: failed to close stream", err)
		}
	}()

	// write the specifier
	err = modules.RPCWrite(stream, modules.RPCFundAccount)
	if err != nil {
		return modules.FundAccountResponse{}, err
	}

	// send price table uid
	pt := w.staticHostPrices.managedPriceTable()
	err = modules.RPCWrite(stream, pt.UID)
	if err != nil {
		return modules.FundAccountResponse{}, err
	}

	// send fund account request
	err = modules.RPCWrite(stream, modules.FundAccountRequest{Account: w.staticAccount.staticID})
	if err != nil {
		return modules.FundAccountResponse{}, err
	}

	// provide payment
	err = w.renter.hostContractor.ProvidePayment(stream, w.staticHostPubKey, modules.RPCFundAccount, amount.Add(pt.FundAccountCost), modules.ZeroAccountID, bh)
	if err != nil {
		return modules.FundAccountResponse{}, err
	}

	// receive FundAccountResponse
	var resp modules.FundAccountResponse
	err = modules.RPCRead(stream, &resp)
	if err != nil {
		return modules.FundAccountResponse{}, err
	}

	fmt.Println("FUND EA SUCCESS")
	return resp, nil
}

// managedHasSector returns whether or not the host has a sector with given root
func (w *worker) managedHasSector(sectorRoot crypto.Hash) (bool, error) {
	fmt.Println("HAS SECTOR", sectorRoot)
	var hasSector bool
	defer func() {
		fmt.Println("HAS SECTOR", hasSector)
	}()

	// create a new stream
	stream, err := w.staticNewStream()
	if err != nil {
		return false, errors.AddContext(err, "Unable to create a new stream")
	}
	defer func() {
		if err := stream.Close(); err != nil {
			w.renter.log.Println("ERROR: failed to close stream", err)
		}
	}()

	// create the program
	pt := w.staticHostPrices.managedPriceTable()
	pb := modules.NewProgramBuilder(&pt)
	pb.AddHasSectorInstruction(sectorRoot)
	program, programData := pb.Program()
	cost, _, _ := pb.Cost(true)

	// add bandwidth cost
	// TODO: figure out bandwidth cost
	cost = cost.Add(types.SiacoinPrecision.Div64(1e3))

	// exeucte it
	responses, err := w.managedExecuteProgram(program, programData, w.staticHostFCID, cost)
	if err != nil {
		fmt.Printf("execute program err %v\n", err)
		return false, err
	}

	// return the response
	for _, resp := range responses {
		fmt.Printf("processing response, output: %v err: %v\n", resp.Output, resp.Error)
		if resp.Error != nil {
			return false, resp.Error
		}
		hasSector = resp.Output[0] == 1
		break
	}
	return hasSector, nil
}

// managedUpdatePriceTable performs the UpdatePriceTableRPC on the host.
func (w *worker) managedUpdatePriceTable() error {
	// check host version
	if build.VersionCmp(w.staticHostVersion, compatRHProtocolVersion) < 0 {
		build.Critical("Executing new RHP RPC on host with version", w.staticHostVersion)
	}

	// create a new stream
	stream, err := w.staticNewStream()
	if err != nil {
		return errors.AddContext(err, "Unable to create a new stream")
	}
	defer func() {
		if err := stream.Close(); err != nil {
			w.renter.log.Println("ERROR: failed to close stream", err)
		}
	}()

	// grab some variables from the worker
	w.mu.Lock()
	bh := w.cachedBlockHeight
	w.mu.Unlock()

	// write the specifier
	err = modules.RPCWrite(stream, modules.RPCUpdatePriceTable)
	if err != nil {
		return err
	}

	// receive the price table
	var uptr modules.RPCUpdatePriceTableResponse
	err = modules.RPCRead(stream, &uptr)
	if err != nil {
		return err
	}

	// decode the JSON
	var pt modules.RPCPriceTable
	err = json.Unmarshal(uptr.PriceTableJSON, &pt)
	if err != nil {
		return err
	}

	// TODO: (follow-up) perform gouging check
	// TODO: (follow-up) this should negatively affect the host's score

	// provide payment
	err = w.renter.hostContractor.ProvidePayment(stream, w.staticHostPubKey, modules.RPCUpdatePriceTable, pt.UpdatePriceTableCost, w.staticAccount.staticID, bh)
	if err != nil {
		return err
	}

	// update the price table
	w.staticHostPrices.managedUpdatePriceTable(pt)
	fmt.Println("PT RECEIVED", pt)
	return nil
}

// managedReadSector returns the sector data for given root
func (w *worker) managedReadSector(sectorRoot crypto.Hash, offset, length uint64) ([]byte, error) {
	fmt.Println("READ SECTOR", sectorRoot, offset, length)
	// create a new stream
	stream, err := w.staticNewStream()
	if err != nil {
		return nil, errors.AddContext(err, "Unable to create a new stream")
	}
	defer func() {
		if err := stream.Close(); err != nil {
			w.renter.log.Println("ERROR: failed to close stream", err)
		}
	}()

	// create the program
	pt := w.staticHostPrices.managedPriceTable()
	pb := modules.NewProgramBuilder(&pt)
	pb.AddReadSectorInstruction(length, offset, sectorRoot, true)
	program, programData := pb.Program()
	cost, _, _ := pb.Cost(true)

	// add bandwidth cost
	// TODO: figure out bandwidth cost
	cost = cost.Add(types.SiacoinPrecision.Div64(1e3))

	// exeucte it
	responses, err := w.managedExecuteProgram(program, programData, w.staticHostFCID, cost)
	if err != nil {
		fmt.Println("READ SECTOR ERR", err)
		return nil, err
	}

	// return the response
	var sectorData []byte
	for _, resp := range responses {
		if resp.Error != nil {
			return nil, resp.Error
		}
		sectorData = resp.Output
		break
	}
	return sectorData, nil
}

// staticNewStream returns a new stream to the worker's host
func (w *worker) staticNewStream() (siamux.Stream, error) {
	return w.renter.staticMux.NewStream(modules.HostSiaMuxSubscriberName, w.staticHostMuxAddress, modules.SiaPKToMuxPK(w.staticHostPubKey))
}
