package renter

import (
	"fmt"
	"sync"
	"time"

	"gitlab.com/NebulousLabs/errors"
	"gitlab.com/SkynetLabs/skyd/skymodules"
	"gitlab.com/SkynetLabs/skyd/skymodules/renter/filesystem/siadir"
)

var (
	// bubbleWaitInTestTime is the amount of time a test should wait before
	// returning an error in test. This is a conservative value to prevent test
	// timeouts.
	bubbleWaitInTestTime = time.Minute
)

// bubbleBlocking is a helper for the renterTester to call bubble on a directory
// and block until the bubble has executed.
func (rt *renterTester) bubbleBlocking(siaPath skymodules.SiaPath) error {
	complete := rt.renter.staticBubbleScheduler.callQueueBubble(siaPath)
	select {
	case <-complete:
	case <-time.After(bubbleWaitInTestTime):
		return errors.New("test blocked too long for bubble")
	}
	return nil
}

// bubbleAllBlocking is a helper for the renterTester to call bubble on multiple
// directories and block until all the bubbles has executed.
func (rt *renterTester) bubbleAllBlocking(siaPaths []skymodules.SiaPath) (errs error) {
	// Define common variables
	var errMU sync.Mutex
	siaPathChan := make(chan skymodules.SiaPath, numBubbleWorkerThreads)
	var wg sync.WaitGroup

	// Define bubbleWorker to call bubble on siaPaths
	bubbleWorker := func() {
		for siaPath := range siaPathChan {
			err := errors.AddContext(rt.bubbleBlocking(siaPath), fmt.Sprintf("error with bubble on %v", siaPath))
			errMU.Lock()
			errs = errors.Compose(errs, err)
			errMU.Unlock()
		}
	}

	// Launch bubble workers
	for i := 0; i < numBubbleWorkerThreads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			bubbleWorker()
		}()
	}

	// Send siaPaths to bubble workers over the siaPathChan
	for _, siaPath := range siaPaths {
		// renterTester bubble method has timeout protection so no need for it here
		siaPathChan <- siaPath
	}
	return
}

// equalBubbledMetadata is a helper that checks for equality in the siadir
// metadata that gets bubbled
//
// Since we can't check timestamps for equality cause they are often set to
// `time.Now()` by methods, we allow a timestamp to be off by a certain delta.
func equalBubbledMetadata(md1, md2 siadir.Metadata, delta time.Duration) (err error) {
	// Check all the time fields first
	// Check AggregateLastHealthCheckTime
	if !timeEquals(md1.AggregateLastHealthCheckTime, md2.AggregateLastHealthCheckTime, delta) {
		err = errors.Compose(err, fmt.Errorf("AggregateLastHealthCheckTimes not equal %v and %v (%v)", md1.AggregateLastHealthCheckTime, md2.AggregateLastHealthCheckTime, delta))
	}
	// Check AggregateModTime
	if !timeEquals(md2.AggregateModTime, md1.AggregateModTime, delta) {
		err = errors.Compose(err, fmt.Errorf("AggregateModTime not equal %v and %v (%v)", md1.AggregateModTime, md2.AggregateModTime, delta))
	}
	// Check LastHealthCheckTime
	if !timeEquals(md1.LastHealthCheckTime, md2.LastHealthCheckTime, delta) {
		err = errors.Compose(err, fmt.Errorf("LastHealthCheckTimes not equal %v and %v (%v)", md1.LastHealthCheckTime, md2.LastHealthCheckTime, delta))
	}
	// Check ModTime
	if !timeEquals(md2.ModTime, md1.ModTime, delta) {
		err = errors.Compose(err, fmt.Errorf("ModTime not equal %v and %v (%v)", md1.ModTime, md2.ModTime, delta))
	}

	// Check a copy of md2 with the time fields of md1 and check for Equality
	md2Copy := md2
	md2Copy.AggregateLastHealthCheckTime = md1.AggregateLastHealthCheckTime
	md2Copy.AggregateModTime = md1.AggregateModTime
	md2Copy.LastHealthCheckTime = md1.LastHealthCheckTime
	md2Copy.ModTime = md1.ModTime
	return errors.Compose(err, siadir.EqualMetadatas(md1, md2Copy))
}

// timeEquals is a helper function for checking if two times are equal
//
// Since we can't check timestamps for equality cause they are often set to
// `time.Now()` by methods, we allow a timestamp to be off by a certain delta.
func timeEquals(t1, t2 time.Time, delta time.Duration) bool {
	if t1.After(t2) && t1.After(t2.Add(delta)) {
		return false
	}
	if t2.After(t1) && t2.After(t1.Add(delta)) {
		return false
	}
	return true
}
