package renter

import (
	"io"

	"gitlab.com/NebulousLabs/Sia/build"
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
func (w *worker) managedExecuteProgram(p modules.Program, data []byte, fcid types.FileContractID, cost types.Currency) (responses []programResponse, err error) {
	println("... starting program execute")
	// check host version
	cache := w.staticCache()
	if build.VersionCmp(cache.staticHostVersion, minAsyncVersion) < 0 {
		build.Critical("Executing new RHP RPC on host with version", cache.staticHostVersion)
	}
	println("...got past version check")

	// track the withdrawal
	// TODO: this is very naive and does not consider refunds at all
	w.staticAccount.managedTrackWithdrawal(cost)
	println("...withdrawal tracked")
	defer func() {
		println("|||defer attempt?")
		w.staticAccount.managedCommitWithdrawal(cost, err == nil)
		println("|||defer successs?")
	}()

	// create a new stream
	var stream siamux.Stream
	println(" www trying to get a new stream")
	stream, err = w.staticNewStream()
	println(" www got out of new stream")
	if err != nil {
		println("---error opening stream?", err.Error())
		err = errors.AddContext(err, "Unable to create a new stream")
		return
	}
	println("... stream opened")
	defer func() {
		if err := stream.Close(); err != nil {
			w.renter.log.Println("ERROR: failed to close stream", err)
		}
	}()

	// grab some variables from the worker
	bh := cache.staticBlockHeight

	// write the specifier
	println("...writing the execute program thingy")
	err = modules.RPCWrite(stream, modules.RPCExecuteProgram)
	if err != nil {
		return
	}

	// send price table uid
	println("... writing pt")
	pt := w.staticPriceTable().staticPriceTable
	err = modules.RPCWrite(stream, pt.UID)
	if err != nil {
		return
	}

	// prepare the request.
	epr := modules.RPCExecuteProgramRequest{
		FileContractID:    fcid,
		Program:           p,
		ProgramDataLength: uint64(len(data)),
	}

	// provide payment
	println("... providing payment")
	err = w.staticAccount.ProvidePayment(stream, w.staticHostPubKey, modules.RPCUpdatePriceTable, cost, w.staticAccount.staticID, bh)
	if err != nil {
		return
	}

	// send the execute program request.
	err = modules.RPCWrite(stream, epr)
	if err != nil {
		return
	}

	// send the programData.
	println("... writing program data")
	_, err = stream.Write(data)
	if err != nil {
		return
	}

	// TODO we call this manually here in order to save a RT and write the
	// program instructions and data before reading. Remove when !4446 is
	// merged.

	// receive PayByEphemeralAccountResponse
	println("... paying by response")
	var payByResponse modules.PayByEphemeralAccountResponse
	err = modules.RPCRead(stream, &payByResponse)
	if err != nil {
		return
	}

	// read the responses.
	println("... reading responses")
	responses = make([]programResponse, len(epr.Program))
	for i := range responses {
		err = modules.RPCRead(stream, &responses[i])
		if err != nil {
			return
		}

		// Read the output data.
		outputLen := responses[i].OutputLength
		responses[i].Output = make([]byte, outputLen, outputLen)
		_, err = io.ReadFull(stream, responses[i].Output)
		if err != nil {
			return
		}

		// If the response contains an error we are done.
		if responses[i].Error != nil {
			break
		}
	}
	return
}

// staticNewStream returns a new stream to the worker's host
func (w *worker) staticNewStream() (siamux.Stream, error) {
	return w.renter.staticMux.NewStream(modules.HostSiaMuxSubscriberName, w.staticHostMuxAddress, modules.SiaPKToMuxPK(w.staticHostPubKey))
}