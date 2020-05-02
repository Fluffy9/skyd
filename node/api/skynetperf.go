package api

import (
	"math"
	"time"
)

type (
	// RequestTimeDistribution contains a distribution of requests, bucketed by
	// how long each request took to return. The buckets use an exponential
	// decay to get a good measurement, which means that the number of requests
	// in each bucket may not be a whole number. It also means that the
	// distribution is not reliable until stats collection has been running for
	// a large multiple of the half life of the distribution.
	//
	// Exponential decay is used to limit the memory footprint and computational
	// overhead of stats collection.
	RequestTimeDistribution struct {
		N60ms   float64 `json:"n60ms"` // Requests taking less than 60ms
		N120ms  float64 `json:"n120ms"`
		N240ms  float64 `json:"n240ms"`
		N500ms  float64 `json:"n500ms"`
		N1000ms float64 `json:"n1000ms"`
		N2000ms float64 `json:n2000ms"`
		NLong   float64 `json:nlong"` // Requests taking longer than 2 seconds.
		NErr    float64 `json:nerr"`  // Requests that errored out.
	}

	// HalfLifeDistribution contains a set of RequestTimeDistributions with
	// different half lives, allowing for a more complete picture of how
	// responsive the requests are over time.
	HalfLifeDistribution struct {
		LastUpdate time.Time `json:"lastupdate"`

		OneMinute       RequestTimeDistribution `json:"oneminute"` // Requests with a half life of one minute.
		FiveMinutes     RequestTimeDistribution `json:"fiveminutes"`
		FifteenMinutes  RequestTimeDistribution `json:"fifteenminutes"`
		TwentyFourHours RequestTimeDistribution `json:"twentyfourhours"`
		Lifetime        RequestTimeDistribution `json:"lifetime"`
	}

	// SkynetPerformanceStats contains a set of performance metrics, bucketed by
	// request size and time window, to give a good picture of how well requests
	// are performing on Skynet.
	SkynetPerformanceStats struct {
		Download64KB  HalfLifeDistribution `json:"download64kb"`
		Download1MB   HalfLifeDistribution `json:"download1mb"`
		Download4MB   HalfLifeDistribution `json:"download4mb"`
		DownloadLarge HalfLifeDistribution `json:"downloadlarge"`

		Upload4MB   HalfLifeDistribution `json:"upload4mb"`
		UploadLarge HalfLifeDistribution `json:"uploadlarge"`
	}
)

// NewHalfLifeDistribution initializes and returns a half life distribution
// ready to collect stats.
func NewHalfLifeDistribution() HalfLifeDistribution {
	return HalfLifeDistribution{
		LastUpdate: time.Now(),
	}
}

// NewSkynetPerformanceStats will return a SkynetPerformanceStats object that is
// ready for use.
func NewSkynetPerformanceStats() *SkynetPerformanceStats {
	return &SkynetPerformanceStats{
		Download64KB:  NewHalfLifeDistribution(),
		Download1MB:   NewHalfLifeDistribution(),
		Download4MB:   NewHalfLifeDistribution(),
		DownloadLarge: NewHalfLifeDistribution(),

		Upload4MB:   NewHalfLifeDistribution(),
		UploadLarge: NewHalfLifeDistribution(),
	}
}

// AddRequest will add a request to the half life distribution. Each call to add
// a request will update the bucket.
func (hld *HalfLifeDistribution) AddRequest(speed time.Duration) {
	// Update the bucket so that all of the decay is in place.
	hld.Update()

	// If speed is zero, add this as an error.
	if speed == 0 {
		hld.OneMinute.NErr++
		hld.FiveMinutes.NErr++
		hld.FifteenMinutes.NErr++
		hld.TwentyFourHours.NErr++
		hld.Lifetime.NErr++
		return
	}

	// Add to the appropriate bucket based on time.
	if speed < 60*time.Millisecond {
		hld.OneMinute.N60ms++
		hld.FiveMinutes.N60ms++
		hld.FifteenMinutes.N60ms++
		hld.TwentyFourHours.N60ms++
		hld.Lifetime.N60ms++
		return
	}
	if speed < 120*time.Millisecond {
		hld.OneMinute.N120ms++
		hld.FiveMinutes.N120ms++
		hld.FifteenMinutes.N120ms++
		hld.TwentyFourHours.N120ms++
		hld.Lifetime.N120ms++
		return
	}
	if speed < 240*time.Millisecond {
		hld.OneMinute.N240ms++
		hld.FiveMinutes.N240ms++
		hld.FifteenMinutes.N240ms++
		hld.TwentyFourHours.N240ms++
		hld.Lifetime.N240ms++
		return
	}
	if speed < 500*time.Millisecond {
		hld.OneMinute.N500ms++
		hld.FiveMinutes.N500ms++
		hld.FifteenMinutes.N500ms++
		hld.TwentyFourHours.N500ms++
		hld.Lifetime.N500ms++
		return
	}
	if speed < 1000*time.Millisecond {
		hld.OneMinute.N1000ms++
		hld.FiveMinutes.N1000ms++
		hld.FifteenMinutes.N1000ms++
		hld.TwentyFourHours.N1000ms++
		hld.Lifetime.N1000ms++
		return
	}
	if speed < 2000*time.Millisecond {
		hld.OneMinute.N2000ms++
		hld.FiveMinutes.N2000ms++
		hld.FifteenMinutes.N2000ms++
		hld.TwentyFourHours.N2000ms++
		hld.Lifetime.N2000ms++
		return
	}

	// Request is too slow to fit in a timed bucket, put it in the long bucket.
	hld.OneMinute.NLong++
	hld.FiveMinutes.NLong++
	hld.FifteenMinutes.NLong++
	hld.TwentyFourHours.NLong++
	hld.Lifetime.NLong++
	return
}

// Update will update the 'LastUpdate' for each bucket, applying the appropriate
// exponential decay to each counter. This should be called before collecting
// stats.
func (hld *HalfLifeDistribution) Update() {
	timePassed := float64(time.Since(hld.LastUpdate))
	hld.LastUpdate = time.Now()

	oneMinuteMult := math.Pow(0.5, timePassed/float64(time.Minute))
	fiveMinuteMult := math.Pow(0.5, timePassed/float64(5*time.Minute))
	fifteenMinuteMult := math.Pow(0.5, timePassed/float64(15*time.Minute))
	twentyFourHourMult := math.Pow(0.5, timePassed/float64(24*60*time.Minute))

	buckets := []*RequestTimeDistribution{&hld.OneMinute, &hld.FiveMinutes, &hld.FifteenMinutes, &hld.TwentyFourHours}
	multiples := []float64{oneMinuteMult, fiveMinuteMult, fifteenMinuteMult, twentyFourHourMult}
	for i := 0; i < len(multiples); i++ {
		buckets[i].N60ms *= multiples[i]
		buckets[i].N120ms *= multiples[i]
		buckets[i].N240ms *= multiples[i]
		buckets[i].N500ms *= multiples[i]
		buckets[i].N1000ms *= multiples[i]
		buckets[i].N2000ms *= multiples[i]
		buckets[i].NLong *= multiples[i]
		buckets[i].NErr *= multiples[i]
	}
}

// Copy returns a copy of the Skynet performance stats that is safe to pass to
// other threads and callers.
func (sps *SkynetPerformanceStats) Copy() SkynetPerformanceStats {
	// Currently there are no pointers within the struct, it is safe to just
	// return a dereferenced copy.
	return *sps
}
