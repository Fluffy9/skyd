package host

import (
	"encoding/hex"
	"fmt"
	"io"
	"reflect"
	"sync"
	"testing"
	"time"

	"gitlab.com/NebulousLabs/Sia/build"
	"gitlab.com/NebulousLabs/Sia/crypto"
	"gitlab.com/NebulousLabs/Sia/modules"
	"gitlab.com/NebulousLabs/Sia/types"
	"gitlab.com/NebulousLabs/fastrand"
	"gitlab.com/NebulousLabs/siamux"
)

// TestRPCSubscribe is a set of tests related to the registry subscription rpc.
func TestRPCSubscribe(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	t.Parallel()

	// create a blank host tester
	rhp, err := newRenterHostPair(t.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := rhp.Close()
		if err != nil {
			t.Error(err)
		}
	}()

	// Add some space to the host's registry.
	is := rhp.staticHT.host.InternalSettings()
	is.RegistrySize += (modules.RegistryEntrySize * 100)
	err = rhp.staticHT.host.SetInternalSettings(is)
	if err != nil {
		t.Fatal(err)
	}

	// Test the standard flow.
	t.Run("Basic", func(t *testing.T) {
		testRPCSubscribeBasic(t, rhp)
	})
}

// testRPCSubscribeBasic tests subscribing to an entry and unsubscribing without
// hitting any edge cases.
func testRPCSubscribeBasic(t *testing.T, rhp *renterHostPair) {
	// Prepare a listener for the worker.
	notificationReader, notificationWriter := io.Pipe()
	var sub types.Specifier
	fastrand.Read(sub[:])
	var notificationUploaded, notificationDownloaded uint64
	var numNotifications uint64
	var notificationMu sync.Mutex
	err := rhp.staticRenterMux.NewListener(hex.EncodeToString(sub[:]), func(stream siamux.Stream) {
		notificationMu.Lock()
		defer notificationMu.Unlock()
		defer func() {
			if err := stream.Close(); err != nil {
				t.Error(err)
			}
		}()
		numNotifications++

		// Copy the output to the pipe.
		io.Copy(notificationWriter, stream)

		// Collect used bandwidth.
		notificationDownloaded += stream.Limit().Downloaded()
		notificationUploaded += stream.Limit().Uploaded()
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = rhp.staticRenterMux.CloseListener(hex.EncodeToString(sub[:]))
		if err != nil {
			t.Fatal(err)
		}
	}()

	// Create a registry value.
	sk, pk := crypto.GenerateKeyPair()
	var tweak crypto.Hash
	fastrand.Read(tweak[:])
	data := fastrand.Bytes(modules.RegistryDataSize)
	rev := fastrand.Uint64n(1000)
	spk := types.SiaPublicKey{
		Algorithm: types.SignatureEd25519,
		Key:       pk[:],
	}
	expiry := types.BlockHeight(1000)
	rv := modules.NewRegistryValue(tweak, data, rev).Sign(sk)

	// Set it on the host.
	host := rhp.staticHT.host
	_, err = host.RegistryUpdate(rv, spk, expiry)
	if err != nil {
		t.Fatal(err)
	}

	// fund the account.
	_, err = rhp.managedFundEphemeralAccount(rhp.pt.FundAccountCost.Add(modules.DefaultHostExternalSettings().MaxEphemeralAccountBalance), false)
	if err != nil {
		t.Fatal(err)
	}

	// check the account balance.
	expectedBalance := modules.DefaultHostExternalSettings().MaxEphemeralAccountBalance
	if !host.staticAccountManager.callAccountBalance(rhp.staticAccountID).Equals(expectedBalance) {
		t.Fatal("invalid balance", expectedBalance, host.staticAccountManager.callAccountBalance(rhp.staticAccountID))
	}

	// begin the subscription loop.
	initialBudget := expectedBalance.Div64(2)
	stream, err := rhp.BeginSubscription(initialBudget, sub)
	if err != nil {
		t.Fatal(err)
	}
	pt := rhp.managedPriceTable()

	// Prepare a function to compute expected budget.
	l := stream.Limit()
	expectedBudget := func(costs types.Currency) types.Currency {
		notificationMu.Lock()
		defer notificationMu.Unlock()
		upCost := pt.UploadBandwidthCost.Mul64(l.Uploaded() + notificationUploaded)
		downCost := pt.DownloadBandwidthCost.Mul64(l.Downloaded() + notificationDownloaded)
		return initialBudget.Sub(upCost).Sub(downCost).Sub(costs)
	}

	// subsribe to the previously created entry.
	rvInitial, err := rhp.SubcribeToRV(stream, pt, spk, tweak)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(rv, rvInitial) {
		t.Fatal("initial value doesn't match")
	}

	runningCost := modules.MDMSubscribeCost(pt, 1, 1)

	// Make sure that the host got the subscription.
	sid := deriveSubscriptionID(spk, tweak)
	err = build.Retry(100, 100*time.Millisecond, func() error {
		host.staticRegistrySubscriptions.mu.Lock()
		defer host.staticRegistrySubscriptions.mu.Unlock()

		if len(host.staticRegistrySubscriptions.subscriptions) != 1 {
			return fmt.Errorf("invalid number of subscriptions %v != %v", len(host.staticRegistrySubscriptions.subscriptions), 1)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	host.staticRegistrySubscriptions.mu.Lock()
	subInfos, found := host.staticRegistrySubscriptions.subscriptions[sid]
	if !found {
		host.staticRegistrySubscriptions.mu.Unlock()
		t.Fatal("subscription not found for id")
	}
	if len(subInfos) != 1 {
		host.staticRegistrySubscriptions.mu.Unlock()
		t.Fatal("wrong number of subscription infos", len(subInfos), 1)
	}
	var info *subscriptionInfo
	for _, subInfo := range subInfos {
		info = subInfo
		break
	}
	host.staticRegistrySubscriptions.mu.Unlock()

	// The info should have the right fields set.
	info.mu.Lock()
	if info.staticStream == nil {
		t.Error("stream not set")
	}
	if !info.notificationCost.Equals(pt.SubscriptionNotificationCost) {
		t.Error("notification cost in info doesn't match pricetable")
	}
	if !info.staticBudget.Remaining().Equals(expectedBudget(runningCost)) {
		t.Fatalf("host budget doesn't match expected budget %v != %v", info.staticBudget.Remaining(), expectedBudget(types.ZeroCurrency))
	}
	info.mu.Unlock()

	// Update the entry on the host.
	rv.Revision++
	rv = rv.Sign(sk)
	_, err = host.RegistryUpdate(rv, spk, expiry)
	if err != nil {
		t.Fatal(err)
	}

	// Read the notification and make sure it's the right one.
	var snt modules.RPCRegistrySubscriptionNotificationType
	err = modules.RPCRead(notificationReader, &snt)
	if err != nil {
		t.Fatal(err)
	}
	if snt.Type != modules.SubscriptionResponseRegistryValue {
		t.Fatal("notification has wrong type")
	}
	var sneu modules.RPCRegistrySubscriptionNotificationEntryUpdate
	err = modules.RPCRead(notificationReader, &sneu)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(rv, sneu.Entry) {
		t.Fatal("wrong entry in notification")
	}
	runningCost = runningCost.Add(pt.SubscriptionNotificationCost)

	// Check the info again.
	info.mu.Lock()
	if !info.notificationCost.Equals(pt.SubscriptionNotificationCost) {
		t.Error("notification cost in info doesn't match pricetable")
	}
	if !info.staticBudget.Remaining().Equals(expectedBudget(runningCost)) {
		t.Fatalf("host budget doesn't match expected budget %v != %v", info.staticBudget.Remaining(), expectedBudget(types.ZeroCurrency))
	}
	info.mu.Unlock()

	// Fund the subscription.
	fundAmt := types.NewCurrency64(42)
	err = rhp.FundSubscription(stream, fundAmt)
	if err != nil {
		t.Fatal(err)
	}
	runningCost = runningCost.Sub(fundAmt)

	// Check the info.
	info.mu.Lock()
	err = build.Retry(100, 100*time.Millisecond, func() error {
		if !info.staticBudget.Remaining().Equals(expectedBudget(runningCost)) {
			return fmt.Errorf("host budget doesn't match expected budget %v != %v", info.staticBudget.Remaining(), expectedBudget(runningCost))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	info.mu.Unlock()

	// Extend the subscription.
	err = rhp.ExtendSubscription(stream, pt)
	if err != nil {
		t.Fatal(err)
	}
	runningCost = runningCost.Add(modules.MDMSubscriptionMemoryCost(pt, 1))

	// Read the "OK" response.
	err = modules.RPCRead(notificationReader, &snt)
	if err != nil {
		t.Fatal(err)
	}
	if snt.Type != modules.SubscriptionResponseSubscriptionSuccess {
		t.Fatal("notification has wrong type")
	}

	// Check the info.
	info.mu.Lock()
	err = build.Retry(100, 100*time.Millisecond, func() error {
		if !info.staticBudget.Remaining().Equals(expectedBudget(runningCost)) {
			return fmt.Errorf("host budget doesn't match expected budget %v != %v", info.staticBudget.Remaining(), expectedBudget(runningCost))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	info.mu.Unlock()

	// Unsubscribe.
	err = rhp.UnsubcribeFromRV(stream, pt, spk, tweak)
	if err != nil {
		t.Fatal(err)
	}
	err = build.Retry(100, 100*time.Millisecond, func() error {
		host.staticRegistrySubscriptions.mu.Lock()
		defer host.staticRegistrySubscriptions.mu.Unlock()

		if len(host.staticRegistrySubscriptions.subscriptions) != 0 {
			return fmt.Errorf("invalid number of subscriptions %v != %v", len(host.staticRegistrySubscriptions.subscriptions), 0)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Update the entry on the host.
	rv.Revision++
	rv = rv.Sign(sk)
	_, err = host.RegistryUpdate(rv, spk, expiry)
	if err != nil {
		t.Fatal(err)
	}

	// Check the info.
	info.mu.Lock()
	err = build.Retry(100, 100*time.Millisecond, func() error {
		if !info.staticBudget.Remaining().Equals(expectedBudget(runningCost)) {
			return fmt.Errorf("host budget doesn't match expected budget %v != %v", info.staticBudget.Remaining(), expectedBudget(runningCost))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	info.mu.Unlock()

	// Close the subscription.
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}

	// Check the balance.
	// 1. subtract the initial budget.
	// 2. add the remaining budget.
	// 3. subtract fundAmt.
	err = build.Retry(100, 100*time.Millisecond, func() error {
		expectedBalance := expectedBalance.Sub(initialBudget)
		expectedBalance = expectedBalance.Add(expectedBudget(runningCost))
		expectedBalance = expectedBalance.Sub(fundAmt)
		if !host.staticAccountManager.callAccountBalance(rhp.staticAccountID).Equals(expectedBalance) {
			return fmt.Errorf("invalid balance %v != %v", expectedBalance, host.staticAccountManager.callAccountBalance(rhp.staticAccountID))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Check number of notifications.
	notificationMu.Lock()
	if expected := numNotifications; expected != 2 {
		t.Error("wrong number of notifications", expected, 2)
	}
	notificationMu.Unlock()
}
