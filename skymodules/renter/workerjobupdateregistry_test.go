package renter

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/opentracing/opentracing-go"
	"gitlab.com/NebulousLabs/errors"
	"gitlab.com/NebulousLabs/fastrand"
	"gitlab.com/SkynetLabs/skyd/siatest/dependencies"
	"gitlab.com/SkynetLabs/skyd/skymodules"
	"go.sia.tech/siad/crypto"
	"go.sia.tech/siad/modules"
)

// TestUpdateRegistryJob tests the various cases of running an UpdateRegistry
// job on a host.
func TestUpdateRegistryJob(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	t.Parallel()

	deps := dependencies.NewDependencyCorruptMDMOutput()
	wt, err := newWorkerTesterCustomDependency(t.Name(), skymodules.SkydProdDependencies, deps)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := wt.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	// Create a registry value.
	rv, spk, sk := randomRegistryValue()
	sid := modules.DeriveRegistryEntryID(spk, rv.Tweak)

	// Run the UpdateRegistry job.
	err = wt.UpdateRegistry(context.Background(), spk, rv)
	if err != nil {
		t.Fatal(err)
	}

	// Manually try to read the entry from the host.
	span := opentracing.GlobalTracer().StartSpan(t.Name())
	lookedUpRV, err := lookupRegistry(wt.worker, sid, &spk, &rv.Tweak)
	span.Finish()
	if err != nil {
		t.Fatal(err)
	}

	// The entries should match.
	if !reflect.DeepEqual(*lookedUpRV, rv) {
		t.Fatal("entries don't match")
	}

	// Run the UpdateRegistry job again with the same entry. Should succeed.
	err = wt.UpdateRegistry(context.Background(), spk, rv)
	if err != nil {
		t.Fatal(err)
	}

	// Run it again with the same revision number but more pow. Should succeed.
	rvMoreWork := rv
	for !rvMoreWork.HasMoreWork(rv.RegistryValue) {
		rvMoreWork.Data = fastrand.Bytes(10)
		rvMoreWork = rvMoreWork.Sign(sk)
	}
	err = wt.UpdateRegistry(context.Background(), spk, rvMoreWork)
	if err != nil {
		t.Fatal(err)
	}
	rv = rvMoreWork

	// Run it again with the same revision number but less pow. Should fail.
	rvLessWork := rv
	for !rv.HasMoreWork(rvLessWork.RegistryValue) {
		rvLessWork.Data = fastrand.Bytes(10)
		rvLessWork = rvLessWork.Sign(sk)
	}
	err = wt.UpdateRegistry(context.Background(), spk, rvLessWork)
	if !errors.Contains(err, modules.ErrInsufficientWork) {
		t.Fatal(err)
	}

	// Make sure there is no recent error or cooldown.
	wt.staticJobUpdateRegistryQueue.mu.Lock()
	if wt.staticJobUpdateRegistryQueue.recentErr != nil {
		t.Fatal("recentErr is set", wt.staticJobUpdateRegistryQueue.recentErr)
	}
	if wt.staticJobUpdateRegistryQueue.cooldownUntil != (time.Time{}) {
		t.Fatal("cooldownUntil is set", wt.staticJobUpdateRegistryQueue.cooldownUntil)
	}
	wt.staticJobUpdateRegistryQueue.mu.Unlock()

	// Same thing again but corrupt the output.
	deps.Fail()
	err = wt.UpdateRegistry(context.Background(), spk, rv)
	deps.Disable()
	if !errors.Contains(err, crypto.ErrInvalidSignature) {
		t.Fatal(err)
	}

	// Make sure the recent error is an invalid signature error and reset the
	// cooldown.
	wt.staticJobUpdateRegistryQueue.mu.Lock()
	if !errors.Contains(wt.staticJobUpdateRegistryQueue.recentErr, crypto.ErrInvalidSignature) {
		t.Fatal(err)
	}
	if wt.staticJobUpdateRegistryQueue.cooldownUntil == (time.Time{}) {
		t.Fatal("coolDown not set")
	}
	wt.staticJobUpdateRegistryQueue.cooldownUntil = time.Time{}
	wt.staticJobUpdateRegistryQueue.recentErr = nil
	wt.staticJobUpdateRegistryQueue.mu.Unlock()

	// Run the UpdateRegistry job with a lower revision number. This time it
	// should fail with an error indicating that the revision number already
	// exists.
	rvLowRevNum := rv
	rvLowRevNum.Revision--
	rvLowRevNum = rvLowRevNum.Sign(sk)
	err = wt.UpdateRegistry(context.Background(), spk, rvLowRevNum)
	if !errors.Contains(err, modules.ErrLowerRevNum) {
		t.Fatal(err)
	}

	// Make sure there is no recent error or cooldown.
	wt.staticJobUpdateRegistryQueue.mu.Lock()
	if wt.staticJobUpdateRegistryQueue.recentErr != nil {
		t.Fatal("recentErr is set", wt.staticJobUpdateRegistryQueue.recentErr)
	}
	if wt.staticJobUpdateRegistryQueue.cooldownUntil != (time.Time{}) {
		t.Fatal("cooldownUntil is set", wt.staticJobUpdateRegistryQueue.cooldownUntil)
	}
	wt.staticJobUpdateRegistryQueue.mu.Unlock()

	// Same thing again but corrupt the output.
	deps.Fail()
	err = wt.UpdateRegistry(context.Background(), spk, rvLowRevNum)
	deps.Disable()
	if !errors.Contains(err, crypto.ErrInvalidSignature) {
		t.Fatal(err)
	}
	if modules.IsRegistryEntryExistErr(err) {
		t.Fatal("Revision error should have been stripped", err)
	}

	// Make sure the recent error is an invalid signature error and reset the
	// cooldown.
	wt.staticJobUpdateRegistryQueue.mu.Lock()
	if !errors.Contains(wt.staticJobUpdateRegistryQueue.recentErr, crypto.ErrInvalidSignature) {
		t.Fatal(err)
	}
	if modules.IsRegistryEntryExistErr(err) {
		t.Fatal("Revision error should have been stripped", err)
	}
	if wt.staticJobUpdateRegistryQueue.cooldownUntil == (time.Time{}) {
		t.Fatal("coolDown not set")
	}
	wt.staticJobUpdateRegistryQueue.cooldownUntil = time.Time{}
	wt.staticJobUpdateRegistryQueue.recentErr = nil
	wt.staticJobUpdateRegistryQueue.mu.Unlock()

	// Manually try to read the entry from the host.
	lookedUpRV, err = lookupRegistry(wt.worker, sid, &spk, &rv.Tweak)
	if err != nil {
		t.Fatal(err)
	}

	// The entries should match.
	if !reflect.DeepEqual(*lookedUpRV, rv) {
		t.Fatal("entries don't match")
	}

	// Increment the revision number and do it one more time.
	rv.Revision++
	rv = rv.Sign(sk)
	err = wt.UpdateRegistry(context.Background(), spk, rv)
	if err != nil {
		t.Fatal(err)
	}

	// Manually try to read the entry from the host.
	lookedUpRV, err = lookupRegistry(wt.worker, sid, &spk, &rv.Tweak)
	if err != nil {
		t.Fatal(err)
	}

	// The entries should match.
	if !reflect.DeepEqual(*lookedUpRV, rv) {
		t.Fatal("entries don't match")
	}
}

// TestUpdateRegistryLyingHost tests the edge case where a host returns a valid
// registry entry but also returns an error.
func TestUpdateRegistryLyingHost(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	t.Parallel()

	wt, err := newWorkerTesterCustomDependency(t.Name(), skymodules.SkydProdDependencies, &dependencies.DependencyRegistryUpdateLyingHost{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := wt.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	// Create a registry value.
	rv, spk, sk := randomRegistryValue()
	sid := modules.DeriveRegistryEntryID(spk, rv.Tweak)

	// Run the UpdateRegistry job.
	err = wt.UpdateRegistry(context.Background(), spk, rv)
	if err != nil {
		t.Fatal(err)
	}

	// Manually try to read the entry from the host.
	lookedUpRV, err := lookupRegistry(wt.worker, sid, &spk, &rv.Tweak)
	if err != nil {
		t.Fatal(err)
	}

	// The entries should match.
	if !reflect.DeepEqual(*lookedUpRV, rv) {
		t.Fatal("entries don't match")
	}

	// Increment the revision number.
	rv.Revision++
	rv = rv.Sign(sk)

	// Run the UpdateRegistry job again. This time the host will respond with an
	// error and provide a proof which has a valid signature, but an outdated
	// revision. The worker should detect the cheating host an
	// errHostInvalidProof error but no revision errors.
	err = wt.UpdateRegistry(context.Background(), spk, rv)
	if !errors.Contains(err, errHostOutdatedProof) {
		t.Fatal("worker should return errHostOutdatedProof")
	}
	if modules.IsRegistryEntryExistErr(err) {
		t.Fatal(err)
	}
}

// TestUpdateRegistryInvalidCache tests the edge case where a host tries to
// prove an invalid revision number with a lower revision number than we have
// stored in the cache for this particular host.
func TestUpdateRegistryInvalidCached(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	t.Parallel()

	deps := dependencies.NewDependencyRegistryUpdateNoOp()
	deps.Disable()
	wt, err := newWorkerTesterCustomDependency(t.Name(), skymodules.SkydProdDependencies, deps)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := wt.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	// Create a registry value.
	rv, spk, sk := randomRegistryValue()

	// Run the UpdateRegistry job.
	err = wt.UpdateRegistry(context.Background(), spk, rv)
	if err != nil {
		t.Fatal(err)
	}

	// Run the UpdateRegistry job again. This time it's a no-op. The renter
	// won't know and increment the revision in the cache.
	rv.Revision++
	rv = rv.Sign(sk)
	deps.Enable()
	err = wt.UpdateRegistry(context.Background(), spk, rv)
	deps.Disable()
	if err != nil {
		t.Fatal(err)
	}

	// Run the UpdateRegistry job again with a lower rev num than the initial
	// one. Causing a ErrLowerRevNumError. The host will use the latest revision
	// it knows for the proof which is lower than the one in the worker cache.
	rv.Revision -= 2
	rv = rv.Sign(sk)
	err = wt.UpdateRegistry(context.Background(), spk, rv)
	if !errors.Contains(err, errHostCheating) {
		t.Fatal(err)
	}

	// Make sure there is a recent error and cooldown.
	wt.staticJobUpdateRegistryQueue.mu.Lock()
	if !errors.Contains(wt.staticJobUpdateRegistryQueue.recentErr, errHostCheating) {
		t.Fatal("wrong recent error", wt.staticJobUpdateRegistryQueue.recentErr)
	}
	if wt.staticJobUpdateRegistryQueue.cooldownUntil == (time.Time{}) {
		t.Fatal("cooldownUntil is not set")
	}
	wt.staticJobUpdateRegistryQueue.mu.Unlock()
}
