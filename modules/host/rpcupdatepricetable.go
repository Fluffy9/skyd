package host

import (
	"encoding/json"
	"fmt"
	"time"

	"gitlab.com/NebulousLabs/Sia/modules"
	"gitlab.com/NebulousLabs/Sia/types"
	"gitlab.com/NebulousLabs/errors"
	"gitlab.com/NebulousLabs/siamux"
)

// managedRPCUpdatePriceTable deep copies the host's current rpc price table,
// sets an expiry and communicates it back to the rpc caller. The host keeps
// track of every rpc table it hands out by its uuid.
func (h *Host) managedRPCUpdatePriceTable(stream siamux.Stream) error {
	// clone the host's price table and track it
	h.mu.Lock()
	pt, err := h.priceTable.Clone(time.Now().Add(rpcPriceGuaranteePeriod).Unix())
	if err != nil {
		h.mu.Unlock()
		return errors.AddContext(err, "Failed to clone the host price table")
	}
	h.uuidToPriceTable[pt.UUID] = pt
	h.mu.Unlock()

	// json encode the price table
	ptBytes, err := json.Marshal(pt)
	if err != nil {
		return errors.AddContext(err, "Failed to JSON encode the price table")
	}

	// send it to the renter
	uptResp := modules.RPCUpdatePriceTableResponse{PriceTableJSON: ptBytes}
	if err = modules.RPCWrite(stream, uptResp); err != nil {
		return errors.AddContext(err, "Failed to write response")
	}

	// Note: we have sent the price table before processing payment for this
	// RPC. This allows the renter to check for price gouging and close out the
	// stream if it does not agree with pricing. After this the host processes
	// payment, and the renter will pay for the RPC according to the price it
	// just received. This essentially means the host is optimistically sending
	// over the price table, which is ok.

	// process payment
	pp := h.NewPaymentProcessor()
	amountPaid, err := pp.ProcessPaymentForRPC(stream)
	if err != nil {
		return errors.AddContext(err, "Failed to process payment")
	}

	// verify payment
	expected := pt.UpdatePriceTableCost
	if amountPaid.Cmp(expected) < 0 {
		return errors.AddContext(modules.ErrInsufficientPaymentForRPC, fmt.Sprintf("The renter did not supply sufficient payment to cover the cost of the  UpdatePriceTableRPC. Expected: %v Actual: %v", expected.HumanString(), amountPaid.HumanString()))
	}

	return nil
}

// managedCalculateUpdatePriceTableCost calculates the price for the
// UpdatePriceTableRPC. The price can be dependant on numerous factors.
// Note: for now this is a fixed cost equaling the base RPC price.
func (h *Host) managedCalculateUpdatePriceTableCost() types.Currency {
	hIS := h.InternalSettings()
	return hIS.MinBaseRPCPrice
}
