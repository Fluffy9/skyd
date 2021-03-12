package host

import (
	"gitlab.com/NebulousLabs/errors"
	"gitlab.com/NebulousLabs/siamux"
	"gitlab.com/skynetlabs/skyd/skymodules"
)

// managedRPCAccountBalance handles the RPC which returns the balance of the
// requested account.
// TODO: Should we require a signature for retrieving the balance?
func (h *Host) managedRPCAccountBalance(stream siamux.Stream) error {
	// read the price table
	pt, err := h.staticReadPriceTableID(stream)
	if err != nil {
		return errors.AddContext(err, "failed to read price table")
	}

	// Process payment.
	pd, err := h.ProcessPayment(stream, pt.HostBlockHeight)
	if err != nil {
		return errors.AddContext(err, "failed to process payment")
	}

	// Check payment.
	if pd.Amount().Cmp(pt.AccountBalanceCost) < 0 {
		return skymodules.ErrInsufficientPaymentForRPC
	}

	// Refund excessive payment.
	refund := pd.Amount().Sub(pt.AccountBalanceCost)
	err = h.staticAccountManager.callRefund(pd.AccountID(), refund)
	if err != nil {
		return errors.AddContext(err, "failed to refund client")
	}

	// Read request
	var abr skymodules.AccountBalanceRequest
	err = skymodules.RPCRead(stream, &abr)
	if err != nil {
		return errors.AddContext(err, "Failed to read AccountBalanceRequest")
	}

	// Get account balance.
	balance := h.staticAccountManager.callAccountBalance(abr.Account)

	// Send response.
	err = skymodules.RPCWrite(stream, skymodules.AccountBalanceResponse{
		Balance: balance,
	})
	if err != nil {
		return errors.AddContext(err, "Failed to send AccountBalanceResponse")
	}
	return nil
}