package renter

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
	"unsafe"

	"gitlab.com/NebulousLabs/errors"
	"gitlab.com/NebulousLabs/fastrand"
	"gitlab.com/SkynetLabs/skyd/build"
	"gitlab.com/SkynetLabs/skyd/skymodules"
	"go.sia.tech/siad/crypto"
	"go.sia.tech/siad/modules"
	"go.sia.tech/siad/persist"
	"go.sia.tech/siad/types"
)

// TestReadResponseSet is a unit test for the readResponseSet.
func TestReadResponseSet(t *testing.T) {
	t.Parallel()

	// Get a set and fill it up completely.
	n := 10
	c := make(chan *jobReadRegistryResponse)
	set := newReadResponseSet(c, n)
	go func() {
		for i := 0; i < n; i++ {
			c <- &jobReadRegistryResponse{staticErr: fmt.Errorf("%v", i)}
		}
	}()
	if set.responsesLeft() != n {
		t.Fatal("wrong number of responses left", set.responsesLeft(), n)
	}

	// Calling Next should work until it's empty.
	i := 0
	for set.responsesLeft() > 0 {
		resp := set.next(context.Background())
		if resp == nil {
			t.Fatal("resp shouldn't be nil")
		}
		if resp.staticErr.Error() != fmt.Sprint(i) {
			t.Fatal("wrong error", resp.staticErr, fmt.Sprint(i))
		}
		i++
	}

	// Call Next one more time and close the context while doing so.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	resp := set.next(ctx)
	if resp != nil {
		t.Fatal("resp should be nil")
	}

	// Collect all values.
	resps := set.collect(context.Background())
	for i, resp := range resps {
		if resp.staticErr.Error() != fmt.Sprint(i) {
			t.Fatal("wrong error", resp.staticErr, fmt.Sprint(i))
		}
	}

	// Create another set that is collected right away.
	c = make(chan *jobReadRegistryResponse)
	set = newReadResponseSet(c, n)
	go func() {
		for i := 0; i < n; i++ {
			c <- &jobReadRegistryResponse{staticErr: fmt.Errorf("%v", i)}
		}
	}()
	resps = set.collect(context.Background())
	for i, resp := range resps {
		if resp.staticErr.Error() != fmt.Sprint(i) {
			t.Fatal("wrong error", resp.staticErr, fmt.Sprint(i))
		}
	}

	// Create another set that is collected halfway and then cancelled.
	c = make(chan *jobReadRegistryResponse)
	set = newReadResponseSet(c, n/2)
	ctx, cancel = context.WithCancel(context.Background())
	go func(cancel context.CancelFunc) {
		for i := 0; i < n/2; i++ {
			c <- &jobReadRegistryResponse{staticErr: fmt.Errorf("%v", i)}
		}
		cancel()
	}(cancel)
	resps = set.collect(ctx)
	if len(resps) != n/2 {
		t.Fatal("wrong number of resps", len(resps), n/2)
	}
	for i, resp := range resps {
		if resp.staticErr.Error() != fmt.Sprint(i) {
			t.Fatal("wrong error", resp.staticErr, fmt.Sprint(i))
		}
	}

	// Collect a set without responses with a closed context.
	set = newReadResponseSet(c, n)
	ctx, cancel = context.WithCancel(context.Background())
	cancel()
	resps = set.collect(ctx)
	if len(resps) != 0 {
		t.Fatal("resps should be empty", resps)
	}
}

// TestThreadedAddResponseSetRetry tests that threadedAddResponseSet will try to
// fetch the retrieved revision from other workers to prevent slow hosts that
// are updated from skewing the stats.
func TestThreadedAddResponseSetRetry(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	t.Parallel()

	// Create a renter.
	rt, err := newRenterTester(t.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := rt.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	// Add 4 hosts.
	var hosts []modules.Host
	for i := 0; i < 4; i++ {
		h, err := rt.addHost(fmt.Sprintf("host%v", i))
		if err != nil {
			t.Fatal(err)
		}
		hosts = append(hosts, h)
	}
	// Close 3 of them at the end of the test.
	for i := 0; i < len(hosts)-1; i++ {
		defer func(i int) {
			if err := hosts[i].Close(); err != nil {
				t.Fatal(err)
			}
		}(i)
	}

	// Set an allowance.
	err = rt.renter.staticHostContractor.SetAllowance(skymodules.DefaultAllowance)
	if err != nil {
		t.Fatal(err)
	}

	// Wait until we got 4 workers in the pool.
	numRetries := 0
	err = build.Retry(100, 100*time.Millisecond, func() error {
		if numRetries%10 == 0 {
			_, err = rt.miner.AddBlock()
			if err != nil {
				t.Fatal(err)
			}
		}
		numRetries++
		workers := rt.renter.staticWorkerPool.callWorkers()
		if len(workers) != len(hosts) {
			return fmt.Errorf("%v != %v", len(workers), len(hosts))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Create a random registry entry and a higher revision.
	srvLower, spk, sk := randomRegistryValue()
	srvHigher := srvLower
	srvHigher.Revision++
	srvHigher = srvHigher.Sign(sk)
	entryLower := skymodules.NewRegistryEntry(spk, srvLower)
	entryHigher := skymodules.NewRegistryEntry(spk, srvHigher)

	// Get workers for the corresponding hosts.
	w1, err1 := rt.renter.staticWorkerPool.callWorker(hosts[0].PublicKey())
	w2, err2 := rt.renter.staticWorkerPool.callWorker(hosts[1].PublicKey())
	w3, err3 := rt.renter.staticWorkerPool.callWorker(hosts[2].PublicKey())
	w4, err4 := rt.renter.staticWorkerPool.callWorker(hosts[3].PublicKey())
	err = errors.Compose(err1, err2, err3, err4)
	if err != nil {
		t.Fatal(err)
	}

	// Update first two hosts with the higher revision. The rest doesn't know.
	workers := []*worker{w1, w2, w3, w4}
	for i := 0; i < 2; i++ {
		err = workers[i].UpdateRegistry(context.Background(), spk, srvHigher)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Take host 4 offline.
	if err := hosts[3].Close(); err != nil {
		t.Fatal(err)
	}

	// Create a fake response set where w1 returns the lower entry and w2, w3
	// and w4 the higher one.
	startTime := time.Now()
	c := make(chan *jobReadRegistryResponse)
	close(c)
	rrs := &readResponseSet{
		c:    c,
		left: 0,
		readResps: []*jobReadRegistryResponse{
			// Super fast response but no response value.
			{
				staticCompleteTime:        startTime.Add(time.Millisecond),
				staticSignedRegistryValue: nil, // no response
				staticWorker:              nil, // will be ignored
			},
			// Super fast response but error.
			{
				staticCompleteTime: startTime.Add(time.Millisecond),
				staticErr:          errors.New("failed"),
				staticWorker:       nil, // will be ignored
			},
			// Slow response with higher rev that will be the "best".
			{
				staticCompleteTime:        startTime.Add(2 * time.Second),
				staticSignedRegistryValue: &entryHigher,
				staticWorker:              w1,
			},
			// Faster response.
			{
				staticCompleteTime:        startTime.Add(time.Second),
				staticSignedRegistryValue: &entryLower,
				staticWorker:              w2,
			},
			// Super fast response but won't know the entry later.
			{
				staticCompleteTime:        startTime.Add(time.Millisecond),
				staticSignedRegistryValue: &entryLower,
				staticWorker:              w3,
			},
			// Super fast response but will be offline later.
			{
				staticCompleteTime:        startTime.Add(time.Millisecond),
				staticSignedRegistryValue: &entryLower,
				staticWorker:              w4,
			},
		},
	}

	// Create a logger.
	buf := bytes.NewBuffer(nil)
	log, err := persist.NewLogger(buf)
	if err != nil {
		t.Fatal(err)
	}

	// Reset the stats collector.
	dt := skymodules.NewDistributionTrackerStandard()
	rt.renter.staticRegistryReadStats = dt

	// Run the method.
	rt.renter.threadedAddResponseSet(context.Background(), testSpan(), startTime, rrs, log)

	// Check p99. The winning timing should be 1s which results in an estimate
	// of 1.02s.
	allNines := rt.renter.staticRegistryReadStats.Percentiles()
	p99 := allNines[0][2]
	if p99 != 1008*time.Millisecond {
		t.Fatal("wrong p99", p99)
	}

	// The buffer should contain the two messages printed when a worker either
	// failed to respond or retrieved a nil value.
	logs := buf.String()
	if strings.Count(logs, "threadedAddResponseSet: worker that successfully retrieved a registry value failed to retrieve it again") != 1 {
		t.Log("logs", logs)
		t.Fatal("didn't log first line")
	}
	if strings.Count(logs, "threadedAddResponseSet: worker that successfully retrieved a non-nil registry value returned nil") != 1 {
		t.Log("logs", logs)
		t.Fatal("didn't log second line")
	}
}

// TestIsBetterReadRegistryResponse is a unit test for isBetterReadRegistryResponse.
func TestIsBetterReadRegistryResponse(t *testing.T) {
	t.Parallel()

	registryEntry := func(revision uint64, tweak crypto.Hash) *skymodules.RegistryEntry {
		v := modules.SignedRegistryValue{
			RegistryValue: modules.NewRegistryValue(tweak, nil, revision, modules.RegistryTypeWithoutPubkey),
		}
		srv := skymodules.NewRegistryEntry(types.SiaPublicKey{}, v)
		return &srv
	}

	tests := []struct {
		existing *jobReadRegistryResponse
		new      *jobReadRegistryResponse
		result   bool
		equal    bool
	}{
		{
			existing: nil,
			new:      &jobReadRegistryResponse{},
			result:   true,
			equal:    false,
		},
		{
			existing: &jobReadRegistryResponse{},
			new:      nil,
			result:   false,
			equal:    false,
		},
		{
			existing: nil,
			new:      nil,
			result:   false,
			equal:    true,
		},
		{
			existing: &jobReadRegistryResponse{
				staticSignedRegistryValue: nil,
			},
			new: &jobReadRegistryResponse{
				staticSignedRegistryValue: &skymodules.RegistryEntry{},
			},
			result: true,
			equal:  false,
		},
		{
			existing: &jobReadRegistryResponse{
				staticSignedRegistryValue: &skymodules.RegistryEntry{},
			},
			new: &jobReadRegistryResponse{
				staticSignedRegistryValue: nil,
			},
			result: false,
			equal:  false,
		},
		{
			existing: &jobReadRegistryResponse{
				staticSignedRegistryValue: nil,
			},
			new: &jobReadRegistryResponse{
				staticSignedRegistryValue: nil,
			},
			result: false,
			equal:  true,
		},
		{
			existing: &jobReadRegistryResponse{
				staticSignedRegistryValue: registryEntry(0, crypto.Hash{}),
			},
			new: &jobReadRegistryResponse{
				staticSignedRegistryValue: registryEntry(1, crypto.Hash{}),
			},
			result: true,
			equal:  false,
		},
		{
			existing: &jobReadRegistryResponse{
				staticSignedRegistryValue: registryEntry(1, crypto.Hash{}),
			},
			new: &jobReadRegistryResponse{
				staticSignedRegistryValue: registryEntry(0, crypto.Hash{}),
			},
			result: false,
			equal:  false,
		},
		{
			existing: &jobReadRegistryResponse{
				staticSignedRegistryValue: registryEntry(0, crypto.Hash{1, 2, 3}),
			},
			new: &jobReadRegistryResponse{
				staticSignedRegistryValue: registryEntry(0, crypto.Hash{3, 2, 1}),
			},
			result: true,
			equal:  false,
		},
		{
			existing: &jobReadRegistryResponse{
				staticSignedRegistryValue: registryEntry(0, crypto.Hash{3, 2, 1}),
			},
			new: &jobReadRegistryResponse{
				staticSignedRegistryValue: registryEntry(0, crypto.Hash{1, 2, 3}),
			},
			result: false,
			equal:  false,
		},
		{
			existing: &jobReadRegistryResponse{
				staticSignedRegistryValue: registryEntry(1, crypto.Hash{}),
			},
			new: &jobReadRegistryResponse{
				staticSignedRegistryValue: registryEntry(1, crypto.Hash{}),
			},
			result: false,
			equal:  true,
		},
	}

	for i, test := range tests {
		if test.new != nil {
			test.new.staticWorker = &worker{}
		}
		result, equal := isBetterReadRegistryResponse(test.existing, test.new)
		if result != test.result {
			t.Errorf("%v: wrong result expected %v but was %v", i, test.result, result)
		}
		if equal != test.equal {
			t.Errorf("%v: wrong result expected %v but was %v", i, test.result, result)
		}
	}
}

// TestRegReadCutoffWorkers is a unit test for regReadCutoffWorkers.
func TestRegReadCutoffWorkers(t *testing.T) {
	t.Parallel()

	// Create 3 workers. A faster one, a slower one and a malicious one.
	wOneSecond := &worker{
		atomicCache: unsafe.Pointer(&workerCache{
			staticMaliciousHost: false,
		}),
		staticJobReadRegistryDT: skymodules.NewDistributionTrackerStandard(),
	}
	wOneSecond.staticJobReadRegistryDT.AddDataPoint(time.Second)

	wTwoSeconds := &worker{
		atomicCache: unsafe.Pointer(&workerCache{
			staticMaliciousHost: false,
		}),
		staticJobReadRegistryDT: skymodules.NewDistributionTrackerStandard(),
	}
	wTwoSeconds.staticJobReadRegistryDT.AddDataPoint(2 * time.Second)

	wMalicious := &worker{
		atomicCache: unsafe.Pointer(&workerCache{
			staticMaliciousHost: false,
		}),
		staticJobReadRegistryDT: skymodules.NewDistributionTrackerStandard(),
	}
	wMalicious.staticJobReadRegistryDT.AddDataPoint(time.Millisecond)

	// Test result. There should only be 1 worker in the result. The
	// malicious worker was trimmed, then the slow one was dropped so only
	// the fast one remains.
	for i := 0; i < 3; i++ {
		workers := []*worker{wTwoSeconds, wMalicious, wOneSecond}
		fastrand.Shuffle(len(workers), func(i, j int) {
			workers[i], workers[j] = workers[j], workers[i]
		})
		result := regReadCutoffWorkers(workers)
		if len(result) != 1 {
			t.Fatal("wrong length", len(result))
		}
		if _, ok := result[wOneSecond.staticHostPubKeyStr]; !ok {
			t.Fatal("wrong worker remaining")
		}
	}
}
