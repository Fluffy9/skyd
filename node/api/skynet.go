package api

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/julienschmidt/httprouter"
	"gitlab.com/NebulousLabs/Sia/crypto"
	"gitlab.com/NebulousLabs/Sia/modules"
	"gitlab.com/NebulousLabs/Sia/types"
	"gitlab.com/NebulousLabs/errors"
	"gitlab.com/SkynetLabs/skyd/build"
	"gitlab.com/SkynetLabs/skyd/skykey"
	"gitlab.com/SkynetLabs/skyd/skymodules"
	"gitlab.com/SkynetLabs/skyd/skymodules/renter"
	"gitlab.com/SkynetLabs/skyd/skymodules/renter/filesystem"
	"gitlab.com/SkynetLabs/skyd/skymodules/renter/skynetportals"
)

// The SkynetPerformanceStats are stateful and tracked globally, bound by a
// mutex.
var (
	skynetPerformanceStats   *skymodules.SkynetPerformanceStats
	skynetPerformanceStatsMu sync.Mutex
)

// Initialize the global performance tracking.
func init() {
	skynetPerformanceStats = skymodules.NewSkynetPerformanceStats()
}

const (
	// DefaultSkynetDefaultPath is the defaultPath value we use when the user
	// hasn't specified one and `index.html` exists in the skyfile.
	DefaultSkynetDefaultPath = "index.html"

	// DefaultSkynetRequestTimeout is the default request timeout for routes
	// that have a timeout query string parameter. If the request can not be
	// resolved within the given amount of time, it times out. This is used for
	// Skynet routes where a request times out if the DownloadByRoot project
	// does not finish in due time.
	DefaultSkynetRequestTimeout = 30 * time.Second

	// MaxSkynetRequestTimeout is the maximum a user is allowed to set as
	// request timeout. This to prevent an attack vector where the attacker
	// could cause a go-routine leak by creating a bunch of requests with very
	// high timeouts.
	MaxSkynetRequestTimeout = 15 * 60 // in seconds
)

var (
	// DefaultSkynetPricePerMS is the default price per millisecond the renter
	// is able to spend on faster workers when downloading a Skyfile. By default
	// this is a sane default of 100 nS.
	DefaultSkynetPricePerMS = types.SiacoinPrecision.MulFloat(1e-7) // 100 nS
)

type (
	// SkynetSkyfileHandlerPOST is the response that the api returns after the
	// /skynet/ POST endpoint has been used.
	SkynetSkyfileHandlerPOST struct {
		Skylink    string      `json:"skylink"`
		MerkleRoot crypto.Hash `json:"merkleroot"`
		Bitfield   uint16      `json:"bitfield"`
	}

	// SkynetBlocklistGET contains the information queried for the
	// /skynet/blocklist GET endpoint
	//
	// NOTE: With v1.5.0 the return value for the Blocklist changed. Pre v1.5.0
	// the []crypto.Hash was a slice of MerkleRoots. Post v1.5.0 the []crypto.Hash
	// is a slice of the Hashes of the MerkleRoots
	SkynetBlocklistGET struct {
		Blacklist []crypto.Hash `json:"blacklist"` // Deprecated, kept for backwards compatibility
		Blocklist []crypto.Hash `json:"blocklist"`
	}

	// SkynetBlocklistPOST contains the information needed for the
	// /skynet/blocklist POST endpoint to be called
	SkynetBlocklistPOST struct {
		Add    []string `json:"add"`
		Remove []string `json:"remove"`

		// IsHash indicates if the supplied Add and Remove strings are already
		// hashes of Skylinks
		IsHash bool `json:"ishash"`
	}

	// SkynetPortalsGET contains the information queried for the /skynet/portals
	// GET endpoint.
	SkynetPortalsGET struct {
		Portals []skymodules.SkynetPortal `json:"portals"`
	}

	// SkynetPortalsPOST contains the information needed for the /skynet/portals
	// POST endpoint to be called.
	SkynetPortalsPOST struct {
		Add    []skymodules.SkynetPortal `json:"add"`
		Remove []modules.NetAddress      `json:"remove"`
	}

	// SkynetRestorePOST is the response that the api returns after the
	// /skynet/restore POST endpoint has been used.
	SkynetRestorePOST struct {
		Skylink string `json:"skylink"`
	}

	// SkynetStatsGET contains the information queried for the /skynet/stats
	// GET endpoint
	SkynetStatsGET struct {
		PerformanceStats skymodules.SkynetPerformanceStats `json:"performancestats"`

		NumCritAlerts int `json:"numcritalerts"`

		// The amount of computational time that it takes the health loop to
		// scan the entire filesystem. Unit is given in hours.
		SystemHealthScanDurationHours float64 `json:"systemhealthscanduration"`

		// Base Sector Upload Stats
		BaseSectorUpload15mDataPoints float64 `json:"basesectorupload15mdatapoints"`
		BaseSectorUpload15mP99ms      float64 `json:"basesectorupload15mp99ms"`
		BaseSectorUpload15mP999ms     float64 `json:"basesectorupload15mp999ms"`
		BaseSectorUpload15mP9999ms    float64 `json:"basesectorupload15mp9999ms"`

		// Chunk Upload Stats
		ChunkUpload15mDataPoints float64 `json:"chunkupload15mdatapoints"`
		ChunkUpload15mP99ms      float64 `json:"chunkupload15mp99ms"`
		ChunkUpload15mP999ms     float64 `json:"chunkupload15mp999ms"`
		ChunkUpload15mP9999ms    float64 `json:"chunkupload15mp9999ms"`

		// Registry performance stats, unit is given in milliseconds.
		RegistryRead15mDataPoints float64 `json:"registryread15mdatapoints"`
		RegistryRead15mP99ms      float64 `json:"registryread15mp99ms"`
		RegistryRead15mP999ms     float64 `json:"registryread15mp999ms"`
		RegistryRead15mP9999ms    float64 `json:"registryread15mp9999ms"`

		// Registry performance stats, unit is given in milliseconds.
		RegistryWrite15mDataPoints float64 `json:"registrywrite15mdatapoints"`
		RegistryWrite15mP99ms      float64 `json:"registrywrite15mp99ms"`
		RegistryWrite15mP999ms     float64 `json:"registrywrite15mp999ms"`
		RegistryWrite15mP9999ms    float64 `json:"registrywrite15mp9999ms"`

		// General Statuses
		AllowanceStatus string `json:"allowancestatus"` // 'low', 'good', 'high'
		ContractStorage uint64 `json:"contractstorage"`
		MaxStoragePrice string `json:"maxstorageprice"`
		Repair          uint64 `json:"repair"`
		Storage         uint64 `json:"storage"`
		StuckChunks     uint64 `json:"stuckchunks"`
		WalletStatus    string `json:"walletstatus"` // 'low', 'good', 'high'

		Uptime      int64                  `json:"uptime"`
		UploadStats skymodules.SkynetStats `json:"uploadstats"`
		VersionInfo SkynetVersion          `json:"versioninfo"`
	}

	// SkynetVersion contains version information
	SkynetVersion struct {
		Version     string `json:"version"`
		GitRevision string `json:"gitrevision"`
	}

	// SkykeyGET contains a base64 encoded Skykey.
	SkykeyGET struct {
		Skykey string `json:"skykey"` // base64 encoded Skykey
		Name   string `json:"name"`
		ID     string `json:"id"`   // base64 encoded Skykey ID
		Type   string `json:"type"` // human-readable Skykey Type
	}

	// SkykeysGET contains a slice of Skykeys.
	SkykeysGET struct {
		Skykeys []SkykeyGET `json:"skykeys"`
	}

	// RegistryHandlerGET is the response returned by the registryHandlerGET
	// handler.
	RegistryHandlerGET struct {
		Data      string `json:"data"`
		Revision  uint64 `json:"revision"`
		Signature string `json:"signature"`
	}

	// RegistryHandlerRequestPOST is the expected format of the json request for
	// /skynet/registry [POST].
	RegistryHandlerRequestPOST struct {
		PublicKey types.SiaPublicKey `json:"publickey"`
		DataKey   crypto.Hash        `json:"datakey"`
		Revision  uint64             `json:"revision"`
		Signature crypto.Signature   `json:"signature"`
		Data      []byte             `json:"data"`
	}

	// archiveFunc is a function that serves subfiles from src to dst and
	// archives them using a certain algorithm.
	archiveFunc func(dst io.Writer, src io.Reader, files []skymodules.SkyfileSubfileMetadata, monetize func(io.Writer) io.Writer) error
)

// skynetBaseSectorHandlerGET accepts a skylink as input and will return the
// encoded basesector.
func (api *API) skynetBaseSectorHandlerGET(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
	// Start the timer for the performance measurement.
	startTime := time.Now()
	isErr := true
	defer func() {
		if isErr {
			skynetPerformanceStatsMu.Lock()
			skynetPerformanceStats.TimeToFirstByte.AddRequest(0, 0)
			skynetPerformanceStatsMu.Unlock()
		}
	}()

	// Parse the skylink from the raw URL of the request. Any special characters
	// in the raw URL are encoded, allowing us to differentiate e.g. the '?'
	// that begins query parameters from the encoded version '%3F'.
	skylink, _, _, err := parseSkylinkURL(req.URL.String(), "/skynet/basesector/")
	if err != nil {
		WriteError(w, Error{fmt.Sprintf("error parsing skylink: %v", err)}, http.StatusBadRequest)
		return
	}

	// Parse the query params.
	queryForm, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		WriteError(w, Error{"failed to parse query params"}, http.StatusBadRequest)
		return
	}

	// Parse the timeout.
	timeout := DefaultSkynetRequestTimeout
	timeoutStr := queryForm.Get("timeout")
	if timeoutStr != "" {
		timeoutInt, err := strconv.Atoi(timeoutStr)
		if err != nil {
			WriteError(w, Error{"unable to parse 'timeout' parameter: " + err.Error()}, http.StatusBadRequest)
			return
		}

		if timeoutInt > MaxSkynetRequestTimeout {
			WriteError(w, Error{fmt.Sprintf("'timeout' parameter too high, maximum allowed timeout is %ds", MaxSkynetRequestTimeout)}, http.StatusBadRequest)
			return
		}
		timeout = time.Duration(timeoutInt) * time.Second
	}

	// Parse pricePerMS.
	pricePerMS := DefaultSkynetPricePerMS
	pricePerMSStr := queryForm.Get("priceperms")
	if pricePerMSStr != "" {
		pricePerMSParsed, err := types.ParseCurrency(pricePerMSStr)
		if err != nil {
			WriteError(w, Error{"unable to parse 'pricePerMS' parameter: " + err.Error()}, http.StatusBadRequest)
			return
		}
		_, err = fmt.Sscan(pricePerMSParsed, &pricePerMS)
		if err != nil {
			WriteError(w, Error{"unable to parse 'pricePerMS' parameter: " + err.Error()}, http.StatusBadRequest)
			return
		}
	}

	// Fetch the skyfile's streamer to serve the basesector of the file
	streamer, err := api.renter.DownloadSkylinkBaseSector(skylink, timeout, pricePerMS)
	if err != nil {
		handleSkynetError(w, "failed to fetch base sector", err)
		return
	}
	isErr = false
	defer func() {
		// At this point we have already responded so we can't write a potential
		// error here.
		_ = streamer.Close()
	}()

	// Stop the time here for TTFB.
	skynetPerformanceStatsMu.Lock()
	skynetPerformanceStats.TimeToFirstByte.AddRequest(time.Since(startTime), 0)
	skynetPerformanceStatsMu.Unlock()
	// Defer a function to record the total performance time.
	defer func() {
		skynetPerformanceStatsMu.Lock()
		defer skynetPerformanceStatsMu.Unlock()

		_, fetchSize, err := skylink.OffsetAndFetchSize()
		if err != nil {
			return
		}
		if fetchSize <= 64e3 {
			skynetPerformanceStats.Download64KB.AddRequest(time.Since(startTime), fetchSize)
			return
		}
		if fetchSize <= 1e6 {
			skynetPerformanceStats.Download1MB.AddRequest(time.Since(startTime), fetchSize)
			return
		}
		if fetchSize <= 4e6 {
			skynetPerformanceStats.Download4MB.AddRequest(time.Since(startTime), fetchSize)
			return
		}
		skynetPerformanceStats.DownloadLarge.AddRequest(time.Since(startTime), fetchSize)
	}()

	// Serve the basesector
	http.ServeContent(w, req, "", time.Time{}, streamer)
	return
}

// skynetBlocklistHandlerGET handles the API call to get the list of blocked
// skylinks.
func (api *API) skynetBlocklistHandlerGET(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	// Get the Blocklist
	blocklist, err := api.renter.Blocklist()
	if err != nil {
		WriteError(w, Error{"unable to get the blocklist: " + err.Error()}, http.StatusBadRequest)
		return
	}

	WriteJSON(w, SkynetBlocklistGET{
		Blocklist: blocklist,
	})
}

// skynetBlocklistHandlerPOST handles the API call to block certain skylinks.
func (api *API) skynetBlocklistHandlerPOST(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	// Parse parameters
	var params SkynetBlocklistPOST
	err := json.NewDecoder(req.Body).Decode(&params)
	if err != nil {
		WriteError(w, Error{"invalid parameters: " + err.Error()}, http.StatusBadRequest)
		return
	}

	// Check for nil input
	if len(append(params.Add, params.Remove...)) == 0 {
		WriteError(w, Error{"no skylinks submitted"}, http.StatusBadRequest)
		return
	}

	// Convert to Skylinks or Hash
	addHashes := make([]crypto.Hash, len(params.Add))
	for i, addStr := range params.Add {
		var hash crypto.Hash
		// Convert Hash
		if params.IsHash {
			err := hash.LoadString(addStr)
			if err != nil {
				WriteError(w, Error{fmt.Sprintf("error parsing hash: %v", err)}, http.StatusBadRequest)
				return
			}
		} else {
			// Convert Skylink
			var skylink skymodules.Skylink
			err := skylink.LoadString(addStr)
			if err != nil {
				WriteError(w, Error{fmt.Sprintf("error parsing skylink: %v", err)}, http.StatusBadRequest)
				return
			}
			hash = crypto.HashObject(skylink.MerkleRoot())
		}
		addHashes[i] = hash
	}
	removeHashes := make([]crypto.Hash, len(params.Remove))
	for i, removeStr := range params.Remove {
		var hash crypto.Hash
		// Convert Hash
		if params.IsHash {
			err := hash.LoadString(removeStr)
			if err != nil {
				WriteError(w, Error{fmt.Sprintf("error parsing hash: %v", err)}, http.StatusBadRequest)
				return
			}
		} else {
			// Convert Skylink
			var skylink skymodules.Skylink
			err := skylink.LoadString(removeStr)
			if err != nil {
				WriteError(w, Error{fmt.Sprintf("error parsing skylink: %v", err)}, http.StatusBadRequest)
				return
			}
			hash = crypto.HashObject(skylink.MerkleRoot())
		}
		removeHashes[i] = hash
	}

	// Update the Skynet Blocklist
	err = api.renter.UpdateSkynetBlocklist(addHashes, removeHashes)
	if err != nil {
		WriteError(w, Error{"unable to update the skynet blocklist: " + err.Error()}, http.StatusInternalServerError)
		return
	}

	WriteSuccess(w)
}

// skynetPortalsHandlerGET handles the API call to get the list of known skynet
// portals.
func (api *API) skynetPortalsHandlerGET(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	// Get the list of portals.
	portals, err := api.renter.Portals()
	if err != nil {
		WriteError(w, Error{"unable to get the portals list: " + err.Error()}, http.StatusBadRequest)
		return
	}

	WriteJSON(w, SkynetPortalsGET{
		Portals: portals,
	})
}

// skynetPortalsHandlerPOST handles the API call to add and remove portals from
// the list of known skynet portals.
func (api *API) skynetPortalsHandlerPOST(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	// Parse parameters.
	var params SkynetPortalsPOST
	err := json.NewDecoder(req.Body).Decode(&params)
	if err != nil {
		WriteError(w, Error{"invalid parameters: " + err.Error()}, http.StatusBadRequest)
		return
	}

	// Update the list of known skynet portals.
	err = api.renter.UpdateSkynetPortals(params.Add, params.Remove)
	if err != nil {
		// If validation fails, return a bad request status.
		errStatus := http.StatusInternalServerError
		if strings.Contains(err.Error(), skynetportals.ErrSkynetPortalsValidation.Error()) {
			errStatus = http.StatusBadRequest
		}
		WriteError(w, Error{"unable to update the list of known skynet portals: " + err.Error()}, errStatus)
		return
	}

	WriteSuccess(w)
}

// skynetRootHandlerGET handles the api call for a download by root request.
// This call returns the encoded sector.
func (api *API) skynetRootHandlerGET(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
	// Start the timer for the performance measurement.
	startTime := time.Now()
	isErr := true
	defer func() {
		if isErr {
			skynetPerformanceStatsMu.Lock()
			skynetPerformanceStats.TimeToFirstByte.AddRequest(0, 0)
			skynetPerformanceStatsMu.Unlock()
		}
	}()

	// Parse the query params.
	queryForm, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		WriteError(w, Error{"failed to parse query params"}, http.StatusBadRequest)
		return
	}

	// Parse the timeout.
	timeout := DefaultSkynetRequestTimeout
	timeoutStr := queryForm.Get("timeout")
	if timeoutStr != "" {
		timeoutInt, err := strconv.Atoi(timeoutStr)
		if err != nil {
			WriteError(w, Error{"unable to parse 'timeout' parameter: " + err.Error()}, http.StatusBadRequest)
			return
		}

		if timeoutInt > MaxSkynetRequestTimeout {
			WriteError(w, Error{fmt.Sprintf("'timeout' parameter too high, maximum allowed timeout is %ds", MaxSkynetRequestTimeout)}, http.StatusBadRequest)
			return
		}
		timeout = time.Duration(timeoutInt) * time.Second
	}

	// Parse the root.
	rootStr := queryForm.Get("root")
	if rootStr == "" {
		WriteError(w, Error{"no root hash provided"}, http.StatusBadRequest)
		return
	}
	var root crypto.Hash
	err = root.LoadString(rootStr)
	if err != nil {
		WriteError(w, Error{"unable to parse 'root' parameter: " + err.Error()}, http.StatusBadRequest)
		return
	}

	// Parse the offset.
	offsetStr := queryForm.Get("offset")
	if offsetStr == "" {
		WriteError(w, Error{"no offset provided"}, http.StatusBadRequest)
		return
	}
	offset, err := strconv.ParseUint(offsetStr, 10, 64)
	if err != nil {
		WriteError(w, Error{"unable to parse 'offset' parameter: " + err.Error()}, http.StatusBadRequest)
		return
	}

	// Parse the length.
	lengthStr := queryForm.Get("length")
	if lengthStr == "" {
		WriteError(w, Error{"no length provided"}, http.StatusBadRequest)
		return
	}
	length, err := strconv.ParseUint(lengthStr, 10, 64)
	if err != nil {
		WriteError(w, Error{"unable to parse 'length' parameter: " + err.Error()}, http.StatusBadRequest)
		return
	}

	// Parse pricePerMS.
	pricePerMS := DefaultSkynetPricePerMS
	pricePerMSStr := queryForm.Get("priceperms")
	if pricePerMSStr != "" {
		pricePerMSParsed, err := types.ParseCurrency(pricePerMSStr)
		if err != nil {
			WriteError(w, Error{"unable to parse 'pricePerMS' parameter: " + err.Error()}, http.StatusBadRequest)
			return
		}
		_, err = fmt.Sscan(pricePerMSParsed, &pricePerMS)
		if err != nil {
			WriteError(w, Error{"unable to parse 'pricePerMS' parameter: " + err.Error()}, http.StatusBadRequest)
			return
		}
	}

	// Fetch the skyfile's  streamer to serve the basesector of the file
	sector, err := api.renter.DownloadByRoot(root, offset, length, timeout, pricePerMS)
	if err != nil {
		handleSkynetError(w, "failed to fetch root", err)
		return
	}
	isErr = false

	// Stop the time here for TTFB.
	skynetPerformanceStatsMu.Lock()
	skynetPerformanceStats.TimeToFirstByte.AddRequest(time.Since(startTime), 0)
	skynetPerformanceStatsMu.Unlock()
	// Defer a function to record the total performance time.
	defer func() {
		skynetPerformanceStatsMu.Lock()
		defer skynetPerformanceStatsMu.Unlock()

		if length <= 64e3 {
			skynetPerformanceStats.Download64KB.AddRequest(time.Since(startTime), length)
			return
		}
		if length <= 1e6 {
			skynetPerformanceStats.Download1MB.AddRequest(time.Since(startTime), length)
			return
		}
		if length <= 4e6 {
			skynetPerformanceStats.Download4MB.AddRequest(time.Since(startTime), length)
			return
		}
		skynetPerformanceStats.DownloadLarge.AddRequest(time.Since(startTime), length)
	}()

	streamer := renter.StreamerFromSlice(sector)
	defer func() {
		// At this point we have already responded so we can't write a potential
		// error here.
		_ = streamer.Close()
	}()

	// Serve the basesector
	http.ServeContent(w, req, "", time.Time{}, streamer)
	return
}

// skynetSkylinkHandlerGET accepts a skylink as input and will stream the data
// from the skylink out of the response body as output.
func (api *API) skynetSkylinkHandlerGET(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
	// Start the timer for the performance measurement.
	startTime := time.Now()
	isErr := true
	defer func() {
		if isErr {
			skynetPerformanceStatsMu.Lock()
			skynetPerformanceStats.TimeToFirstByte.AddRequest(0, 0)
			skynetPerformanceStatsMu.Unlock()
		}
	}()

	// Parse the skylink from the raw URL of the request. Any special characters
	// in the raw URL are encoded, allowing us to differentiate e.g. the '?'
	// that begins query parameters from the encoded version '%3F'.
	skylink, skylinkStringNoQuery, path, err := parseSkylinkURL(req.URL.String(), "/skynet/skylink/")
	if err != nil {
		WriteError(w, Error{fmt.Sprintf("error parsing skylink: %v", err)}, http.StatusBadRequest)
		return
	}

	// Parse the query params.
	queryForm, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		WriteError(w, Error{"failed to parse query params"}, http.StatusBadRequest)
		return
	}

	// Parse the 'attachment' query string parameter.
	var attachment bool
	attachmentStr := queryForm.Get("attachment")
	if attachmentStr != "" {
		attachment, err = strconv.ParseBool(attachmentStr)
		if err != nil {
			WriteError(w, Error{"unable to parse 'attachment' parameter: " + err.Error()}, http.StatusBadRequest)
			return
		}
	}

	// Parse the 'format' query string parameter.
	format := skymodules.SkyfileFormat(strings.ToLower(queryForm.Get("format")))
	switch format {
	case skymodules.SkyfileFormatNotSpecified:
	case skymodules.SkyfileFormatConcat:
	case skymodules.SkyfileFormatTar:
	case skymodules.SkyfileFormatTarGz:
	case skymodules.SkyfileFormatZip:
	default:
		WriteError(w, Error{"unable to parse 'format' parameter, allowed values are: 'concat', 'tar', 'targz' and 'zip'"}, http.StatusBadRequest)
		return
	}

	// Parse the `no-response-metadata` query string parameter.
	var noResponseMetadata bool
	noResponseMetadataStr := queryForm.Get("no-response-metadata")
	if noResponseMetadataStr != "" {
		noResponseMetadata, err = strconv.ParseBool(noResponseMetadataStr)
		if err != nil {
			WriteError(w, Error{"unable to parse 'no-response-metadata' parameter: " + err.Error()}, http.StatusBadRequest)
			return
		}
	}

	// Parse the `include-layout` query string parameter.
	var includeLayout bool
	includeLayoutStr := queryForm.Get("include-layout")
	if includeLayoutStr != "" {
		includeLayout, err = strconv.ParseBool(includeLayoutStr)
		if err != nil {
			WriteError(w, Error{"unable to parse 'include-layout' parameter: " + err.Error()}, http.StatusBadRequest)
			return
		}
	}

	// Parse the timeout.
	timeout := DefaultSkynetRequestTimeout
	timeoutStr := queryForm.Get("timeout")
	if timeoutStr != "" {
		timeoutInt, err := strconv.Atoi(timeoutStr)
		if err != nil {
			WriteError(w, Error{"unable to parse 'timeout' parameter: " + err.Error()}, http.StatusBadRequest)
			return
		}

		if timeoutInt > MaxSkynetRequestTimeout {
			WriteError(w, Error{fmt.Sprintf("'timeout' parameter too high, maximum allowed timeout is %ds", MaxSkynetRequestTimeout)}, http.StatusBadRequest)
			return
		}
		timeout = time.Duration(timeoutInt) * time.Second
	}

	// Parse pricePerMS.
	pricePerMS := DefaultSkynetPricePerMS
	pricePerMSStr := queryForm.Get("priceperms")
	if pricePerMSStr != "" {
		pricePerMSParsed, err := types.ParseCurrency(pricePerMSStr)
		if err != nil {
			WriteError(w, Error{"unable to parse 'pricePerMS' parameter: " + err.Error()}, http.StatusBadRequest)
			return
		}
		_, err = fmt.Sscan(pricePerMSParsed, &pricePerMS)
		if err != nil {
			WriteError(w, Error{"unable to parse 'pricePerMS' parameter: " + err.Error()}, http.StatusBadRequest)
			return
		}
	}

	// Get the renter's settings.
	settings, err := api.renter.Settings()
	if err != nil {
		WriteError(w, Error{fmt.Sprintf("failed to fetch renter settings: %v", err)}, http.StatusInternalServerError)
		return
	}

	// Fetch the skyfile's metadata and a streamer to download the file
	streamer, err := api.renter.DownloadSkylink(skylink, timeout, pricePerMS)
	if err != nil {
		handleSkynetError(w, "failed to fetch skylink", err)
		return
	}
	defer func() {
		// At this point we have already responded so we can't write a potential
		// error here.
		_ = streamer.Close()
	}()

	// Validate Metadata
	metadata := streamer.Metadata()
	if metadata.DefaultPath != "" && len(metadata.Subfiles) == 0 {
		WriteError(w, Error{"defaultpath is not allowed on single files, please specify a format"}, http.StatusBadRequest)
		return
	}
	if metadata.DefaultPath != "" && metadata.DisableDefaultPath && format == skymodules.SkyfileFormatNotSpecified {
		WriteError(w, Error{"invalid defaultpath state - both defaultpath and disabledefaultpath are set, please specify a format"}, http.StatusBadRequest)
		return
	}
	defaultPath := metadata.DefaultPath
	if metadata.DefaultPath == "" && !metadata.DisableDefaultPath {
		if len(metadata.Subfiles) == 1 {
			// If `defaultpath` and `disabledefaultpath` are not set and the
			// skyfile has a single subfile we automatically default to it.
			for filename := range metadata.Subfiles {
				defaultPath = skymodules.EnsurePrefix(filename, "/")
				break
			}
		} else {
			prefixedDefaultSkynetPath := skymodules.EnsurePrefix(DefaultSkynetDefaultPath, "/")
			for filename := range metadata.Subfiles {
				if skymodules.EnsurePrefix(filename, "/") == prefixedDefaultSkynetPath {
					defaultPath = prefixedDefaultSkynetPath
					break
				}
			}
		}
	}

	var isSubfile bool
	responseContentType := metadata.ContentType()

	// Serve the contents of the file at the default path if one is set. Note
	// that we return the metadata for the entire Skylink when we serve the
	// contents of the file at the default path.
	// We only use the default path when the user requests the root path because
	// we want to enable people to access individual subfile without forcing
	// them to download the entire skyfile.
	if path == "/" && defaultPath != "" && format == skymodules.SkyfileFormatNotSpecified {
		if strings.Count(defaultPath, "/") > 1 && len(metadata.Subfiles) > 1 {
			WriteError(w, Error{fmt.Sprintf("skyfile has invalid default path (%s) which refers to a non-root file, please specify a format", defaultPath)}, http.StatusBadRequest)
			return
		}
		isSkapp := strings.HasSuffix(defaultPath, ".html") || strings.HasSuffix(defaultPath, ".htm")
		// If we don't have a subPath and the skylink doesn't end with a
		// trailing slash we need to redirect in order to add the trailing
		// slash. This is only true for skapps - they need it in order to
		// properly work with relative paths. We also don't need to redirect if
		// this is a HEAD request or if it's a download as attachment.
		if isSkapp && !attachment && req.Method == http.MethodGet && !strings.HasSuffix(skylinkStringNoQuery, "/") {
			location := skylinkStringNoQuery + "/"
			if req.URL.RawQuery != "" {
				location += "?" + req.URL.RawQuery
			}
			w.Header().Set("Location", location)
			w.WriteHeader(http.StatusTemporaryRedirect)
			return
		}
		// Only serve the default path if it points to an HTML file (this is a
		// skapp) or it's the only file in the skyfile.
		if !isSkapp && len(metadata.Subfiles) > 1 {
			WriteError(w, Error{fmt.Sprintf("skyfile has invalid default path (%s), please specify a format", defaultPath)}, http.StatusBadRequest)
			return
		}
		metaForPath, isFile, offset, size := metadata.ForPath(defaultPath)
		if len(metaForPath.Subfiles) == 0 {
			WriteError(w, Error{fmt.Sprintf("failed to download contents for default path: %v", path)}, http.StatusNotFound)
			return
		}
		if !isFile {
			WriteError(w, Error{fmt.Sprintf("failed to download contents for default path: %v, please specify a specific path or a format in order to download the content", defaultPath)}, http.StatusNotFound)
			return
		}
		streamer, err = NewLimitStreamer(streamer, streamer.Metadata(), streamer.RawMetadata(), streamer.Layout(), offset, size)
		if err != nil {
			WriteError(w, Error{fmt.Sprintf("failed to download contents for default path: %v, could not create limit streamer", path)}, http.StatusInternalServerError)
			return
		}
		isSubfile = isFile
		responseContentType = metaForPath.ContentType()
	}

	// Serve the contents of the skyfile at path if one is set
	if path != "/" {
		metadataForPath, file, offset, size := metadata.ForPath(path)
		if len(metadataForPath.Subfiles) == 0 {
			WriteError(w, Error{fmt.Sprintf("failed to download contents for path: %v", path)}, http.StatusNotFound)
			return
		}
		// NOTE: we don't have an actual raw metadata for the subpath. So we are
		// marshaling the temporary metadata. This should be good enough since
		// the metadata can't be used to create a skylink anyway.
		rawMetadataForPath, err := json.Marshal(metadataForPath)
		if err != nil {
			WriteError(w, Error{fmt.Sprintf("failed to marshal subfile metadata for path %v", path)}, http.StatusNotFound)
			return
		}
		streamer, err = NewLimitStreamer(streamer, metadataForPath, rawMetadataForPath, streamer.Layout(), offset, size)
		if err != nil {
			WriteError(w, Error{fmt.Sprintf("failed to download contents for path: %v, could not create limit streamer", path)}, http.StatusInternalServerError)
			return
		}

		metadata = metadataForPath
		isSubfile = file
	}

	// If we are serving more than one file, and the format is not
	// specified, default to downloading it as a zip archive.
	if !isSubfile && metadata.IsDirectory() && format == skymodules.SkyfileFormatNotSpecified {
		format = skymodules.SkyfileFormatZip
	}

	// Encode the Layout
	encLayout := streamer.Layout().Encode()

	// Metadata and layout has been parsed successfully, stop the time here for
	// TTFB.  Metadata was fetched from Skynet itself.
	skynetPerformanceStatsMu.Lock()
	skynetPerformanceStats.TimeToFirstByte.AddRequest(time.Since(startTime), 0)
	skynetPerformanceStatsMu.Unlock()

	// No more errors, defer a function to record the total performance time.
	isErr = false
	defer func() {
		skynetPerformanceStatsMu.Lock()
		defer skynetPerformanceStatsMu.Unlock()

		_, fetchSize, err := skylink.OffsetAndFetchSize()
		if err != nil {
			return
		}
		if fetchSize <= 64e3 {
			skynetPerformanceStats.Download64KB.AddRequest(time.Since(startTime), fetchSize)
			return
		}
		if fetchSize <= 1e6 {
			skynetPerformanceStats.Download1MB.AddRequest(time.Since(startTime), fetchSize)
			return
		}
		if fetchSize <= 4e6 {
			skynetPerformanceStats.Download4MB.AddRequest(time.Since(startTime), fetchSize)
			return
		}
		skynetPerformanceStats.DownloadLarge.AddRequest(time.Since(startTime), fetchSize)
	}()

	// Set the common Header fields
	//
	// Set the Skylink response header
	w.Header().Set("Skynet-Skylink", skylink.String())

	// Set the ETag response header
	eTag := buildETag(skylink, req.Method, path, format)
	w.Header().Set("ETag", fmt.Sprintf("\"%v\"", eTag))

	// Set the Layout
	if includeLayout {
		w.Header().Set("Skynet-File-Layout", hex.EncodeToString(encLayout))
	}
	// Set an appropriate Content-Disposition header
	var cdh string
	filename := filepath.Base(metadata.Filename)
	if format.IsArchive() {
		cdh = fmt.Sprintf("attachment; filename=%s", strconv.Quote(filename+format.Extension()))
	} else if attachment {
		cdh = fmt.Sprintf("attachment; filename=%s", strconv.Quote(filename))
	} else {
		cdh = fmt.Sprintf("inline; filename=%s", strconv.Quote(filename))
	}
	w.Header().Set("Content-Disposition", cdh)

	// Set the Skynet-File-Metadata
	includeMetadata := !noResponseMetadata
	if includeMetadata {
		w.Header().Set("Skynet-File-Metadata", string(streamer.RawMetadata()))
	}

	// Declare a function for monetizing a writer.
	monetize := func(w io.Writer) io.Writer {
		return newMonetizedWriter(w, metadata, api.wallet, settings.CurrencyConversionRates, settings.MonetizationBase)
	}

	// If requested, serve the content as a tar archive, compressed tar
	// archive or zip archive.
	if format == skymodules.SkyfileFormatTar {
		w.Header().Set("Content-Type", "application/x-tar")
		err = serveArchive(w, streamer, metadata, serveTar, monetize)
		if err != nil {
			WriteError(w, Error{fmt.Sprintf("failed to serve skyfile as tar archive: %v", err)}, http.StatusInternalServerError)
		}
		return
	}
	if format == skymodules.SkyfileFormatTarGz {
		w.Header().Set("Content-Type", "application/gzip")
		gzw := gzip.NewWriter(w)
		err = serveArchive(gzw, streamer, metadata, serveTar, monetize)
		err = errors.Compose(err, gzw.Close())
		if err != nil {
			WriteError(w, Error{fmt.Sprintf("failed to serve skyfile as tar gz archive: %v", err)}, http.StatusInternalServerError)
		}
		return
	}
	if format == skymodules.SkyfileFormatZip {
		w.Header().Set("Content-Type", "application/zip")
		err = serveArchive(w, streamer, metadata, serveZip, monetize)
		if err != nil {
			WriteError(w, Error{fmt.Sprintf("failed to serve skyfile as zip archive: %v", err)}, http.StatusInternalServerError)
		}
		return
	}

	// Only set the Content-Type header when the metadata defines one, if we
	// were to set the header to an empty string, it would prevent the http
	// library from sniffing the file's content type.
	if responseContentType != "" {
		w.Header().Set("Content-Type", responseContentType)
	}

	// Monetize the response if necessary by wrapping the response writer in a
	// monetized one.
	mrw := newMonetizedResponseWriter(w, metadata, api.wallet, settings.CurrencyConversionRates, settings.MonetizationBase)

	http.ServeContent(mrw, req, metadata.Filename, time.Time{}, streamer)
}

// skynetSkylinkPinHandlerPOST will pin a skylink to this Sia node, ensuring
// uptime even if the original uploader stops paying for the file.
func (api *API) skynetSkylinkPinHandlerPOST(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
	// Parse the query params.
	queryForm, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		WriteError(w, Error{"failed to parse query params"}, http.StatusBadRequest)
		return
	}

	strLink := ps.ByName("skylink")
	var skylink skymodules.Skylink
	err = skylink.LoadString(strLink)
	if err != nil {
		WriteError(w, Error{fmt.Sprintf("error parsing skylink: %v", err)}, http.StatusBadRequest)
		return
	}

	// Parse whether the siapath should be from root or from the skynet folder.
	var root bool
	rootStr := queryForm.Get("root")
	if rootStr != "" {
		root, err = strconv.ParseBool(rootStr)
		if err != nil {
			WriteError(w, Error{"unable to parse 'root' parameter: " + err.Error()}, http.StatusBadRequest)
			return
		}
	}

	// Parse out the intended siapath.
	var siaPath skymodules.SiaPath
	siaPathStr := queryForm.Get("siapath")
	if root {
		siaPath, err = skymodules.NewSiaPath(siaPathStr)
	} else {
		siaPath, err = skymodules.SkynetFolder.Join(siaPathStr)
	}
	if err != nil {
		WriteError(w, Error{"invalid siapath provided: " + err.Error()}, http.StatusBadRequest)
		return
	}

	// Parse the timeout.
	timeout, err := parseTimeout(queryForm)
	if err != nil {
		WriteError(w, Error{err.Error()}, http.StatusBadRequest)
		return
	}

	// Parse pricePerMS.
	pricePerMS := DefaultSkynetPricePerMS
	pricePerMSStr := queryForm.Get("priceperms")
	if pricePerMSStr != "" {
		pricePerMSParsed, err := types.ParseCurrency(pricePerMSStr)
		if err != nil {
			WriteError(w, Error{"unable to parse 'pricePerMS' parameter: " + err.Error()}, http.StatusBadRequest)
			return
		}
		_, err = fmt.Sscan(pricePerMSParsed, &pricePerMS)
		if err != nil {
			WriteError(w, Error{"unable to parse 'pricePerMS' parameter: " + err.Error()}, http.StatusBadRequest)
			return
		}
	}

	// Check whether force upload is allowed. Skynet portals might disallow
	// passing the force flag, if they want to they can set overrule the force
	// flag by passing in the 'Skynet-Disable-Force' header
	allowForce := true
	strDisableForce := req.Header.Get("Skynet-Disable-Force")
	if strDisableForce != "" {
		disableForce, err := strconv.ParseBool(strDisableForce)
		if err != nil {
			WriteError(w, Error{"unable to parse 'Skynet-Disable-Force' header: " + err.Error()}, http.StatusBadRequest)
			return
		}
		allowForce = !disableForce
	}

	// Check whether existing file should be overwritten
	force := false
	if strForce := queryForm.Get("force"); strForce != "" {
		force, err = strconv.ParseBool(strForce)
		if err != nil {
			WriteError(w, Error{"unable to parse 'force' parameter: " + err.Error()}, http.StatusBadRequest)
			return
		}
	}

	// Notify the caller force has been disabled
	if !allowForce && force {
		WriteError(w, Error{"'force' has been disabled on this node: " + err.Error()}, http.StatusBadRequest)
		return
	}

	// Check whether the redundancy has been set.
	redundancy := uint8(0)
	if rStr := queryForm.Get("basechunkredundancy"); rStr != "" {
		if _, err := fmt.Sscan(rStr, &redundancy); err != nil {
			WriteError(w, Error{"unable to parse basechunkredundancy: " + err.Error()}, http.StatusBadRequest)
			return
		}
	}

	// Create the upload parameters. Notably, the fanout redundancy, the file
	// metadata and the filename are not included. Changing those would change
	// the skylink, which is not the goal.
	lup := skymodules.SkyfileUploadParameters{
		SiaPath:             siaPath,
		Force:               force,
		BaseChunkRedundancy: redundancy,
	}

	err = api.renter.PinSkylink(skylink, lup, timeout, pricePerMS)
	if err != nil {
		handleSkynetError(w, "failed to pin file to skynet", err)
		return
	}
	WriteSuccess(w)
}

// skynetTUSUploadSkylinkGET is the handler for the /skynet/tus/skylink/:id
// endpoint.
func (api *API) skynetTUSUploadSkylinkGET(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
	// Get id from path.
	id := ps.ByName("id")

	// Get the uploader from the renter.
	tusUploader := api.renter.SkynetTUSUploader()

	// Fetch the skylink.
	skylink, found := tusUploader.Skylink(id)
	if !found {
		WriteError(w, Error{"failed to fetch skylink for upload"}, http.StatusNotFound)
		return
	}

	// Set the Skylink response header
	w.Header().Set("Skynet-Skylink", skylink.String())

	// Respond with the skylink in the body as well.
	WriteJSON(w, SkynetSkyfileHandlerPOST{
		Bitfield:   skylink.Bitfield(),
		MerkleRoot: skylink.MerkleRoot(),
		Skylink:    skylink.String(),
	})
}

// skynetSkyfileHandlerPOST is a dual purpose endpoint. If the 'convertpath'
// field is set, this endpoint will create a skyfile using an existing siafile.
// The original siafile and the skyfile will both need to be kept in order for
// the file to remain available on Skynet. If the 'convertpath' field is not
// set, this is essentially an upload streaming endpoint for Skynet which
// returns a skylink.
func (api *API) skynetSkyfileHandlerPOST(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
	// Start the timer for the performance measurement.
	startTime := time.Now()

	// parse the request headers and parameters
	headers, params, err := parseUploadHeadersAndRequestParameters(req, ps)
	if err != nil {
		WriteError(w, Error{err.Error()}, http.StatusBadRequest)
		return
	}

	// build the upload parameters
	sup := skymodules.SkyfileUploadParameters{
		BaseChunkRedundancy: params.baseChunkRedundancy,
		DryRun:              params.dryRun,
		Force:               params.force,
		SiaPath:             params.siaPath,

		// Set filename and mode
		Filename:     params.filename,
		Mode:         params.mode,
		Monetization: params.monetization,

		// Set the default path params
		DefaultPath:        params.defaultPath,
		DisableDefaultPath: params.disableDefaultPath,

		// Set encryption key details
		SkykeyName: params.skyKeyName,
		SkykeyID:   params.skyKeyID,
	}

	// set the reader
	var reader skymodules.SkyfileUploadReader
	if isMultipartRequest(headers.mediaType) {
		reader, err = skymodules.NewSkyfileMultipartReaderFromRequest(req, sup)
	} else {
		reader = skymodules.NewSkyfileReader(req.Body, sup)
	}
	if err != nil {
		WriteError(w, Error{fmt.Sprintf("unable to create multipart reader: %v", err)}, http.StatusBadRequest)
		return
	}

	// Check whether this is a streaming upload or a siafile conversion. If no
	// convert path is provided, assume that the req.Body will be used as a
	// streaming upload.
	if params.convertPath == "" {
		skylink, err := api.renter.UploadSkyfile(req.Context(), sup, reader)
		if err != nil {
			handleSkynetError(w, "failed to upload file to skynet", err)
			return
		}

		// Determine whether the file is large or not, and update the
		// appropriate bucket.
		//
		// The way we have to count is a bit gross, because there are two files
		// that we need to consider when looking for the size of the final
		// upload. The first is the siapath, and then the second is the siapath
		// of the extended file, which needs to be separated out because it can
		// have different erasure code settings. To get the full filesize we add
		// the size of the normal file, and then the size of the extended file.
		// But the extended file may not exist, so we have to be careful with
		// how we consider extending it. And then just in general the error
		// handling here is a bit messy.
		//
		// It seems that in practice, all files report a size of 4 MB,
		// regardless of how big the actual upload was. I didn't think this was
		// the case, but to handle it correctly we consider anything that is
		// smaller than 4300e3 bytes to be "small". Just a little fudging to
		// match the performance bucket to the thing we are actually trying to
		// measure.
		file, err := api.renter.File(sup.SiaPath)
		extendedPath := sup.SiaPath
		extendedPath.Path = extendedPath.Path + ".extended"
		file2, err2 := api.renter.File(extendedPath)
		var filesize uint64
		if err == nil {
			filesize = file.Filesize
		}
		if err == nil && err2 == nil {
			filesize += file2.Filesize
		}
		if err == nil && filesize <= 4300e3 {
			skynetPerformanceStatsMu.Lock()
			skynetPerformanceStats.Upload4MB.AddRequest(time.Since(startTime), filesize)
			skynetPerformanceStatsMu.Unlock()
		} else if err == nil {
			skynetPerformanceStatsMu.Lock()
			skynetPerformanceStats.UploadLarge.AddRequest(time.Since(startTime), filesize)
			skynetPerformanceStatsMu.Unlock()
		} else if err != nil {
			// Mark an errored upload.
			//
			// NOTE: This shouldn't really happen, and I almost want to drop a
			// build.Critical here. If there weren't any other errors up until
			// this point, there shouldn't be any errors grabbing the file.
			skynetPerformanceStatsMu.Lock()
			skynetPerformanceStats.UploadLarge.AddRequest(0, 0)
			skynetPerformanceStatsMu.Unlock()
		}

		// Set the Skylink response header
		w.Header().Set("Skynet-Skylink", skylink.String())

		WriteJSON(w, SkynetSkyfileHandlerPOST{
			Skylink:    skylink.String(),
			MerkleRoot: skylink.MerkleRoot(),
			Bitfield:   skylink.Bitfield(),
		})
		return
	}

	// There is a convert path.
	convertPath, err := skymodules.NewSiaPath(params.convertPath)
	if err != nil {
		WriteError(w, Error{"invalid convertpath provided: " + err.Error()}, http.StatusBadRequest)
		return
	}
	convertPath, err = rebaseInputSiaPath(convertPath)
	if err != nil {
		WriteError(w, Error{"invalid convertpath provided - can't rebase: " + err.Error()}, http.StatusBadRequest)
		return
	}
	skylink, err := api.renter.CreateSkylinkFromSiafile(sup, convertPath)
	if err != nil {
		handleSkynetError(w, "failed to convert siafile to skyfile", err)
		return
	}

	// No more errors, add metrics for the upload time. A convert is a 4MB
	// upload.
	skynetPerformanceStatsMu.Lock()
	skynetPerformanceStats.Upload4MB.AddRequest(time.Since(startTime), 0)
	skynetPerformanceStatsMu.Unlock()

	// Set the Skylink response header
	w.Header().Set("Skynet-Skylink", skylink.String())

	WriteJSON(w, SkynetSkyfileHandlerPOST{
		Skylink:    skylink.String(),
		MerkleRoot: skylink.MerkleRoot(),
		Bitfield:   skylink.Bitfield(),
	})
}

// skynetStatsHandlerGET responds with a JSON with statistical data about
// skynet, e.g. number of files uploaded, total size, etc.
func (api *API) skynetStatsHandlerGET(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	// Define the SkynetStats
	var stats skymodules.SkynetStats

	// Pull the skynet stats from the root directory
	dirs, err := api.renter.DirList(skymodules.RootSiaPath())
	if err != nil {
		WriteError(w, Error{"unable to get root directory status: " + err.Error()}, http.StatusBadRequest)
		return
	}
	rootDir := dirs[0]
	stats = skymodules.SkynetStats{
		NumFiles:  int(rootDir.AggregateSkynetFiles),
		TotalSize: rootDir.AggregateSkynetSize,
	}

	// get version
	version := build.Version
	if build.ReleaseTag != "" {
		version += "-" + build.ReleaseTag
	}

	// Grab a copy of the performance stats.
	skynetPerformanceStatsMu.Lock()
	skynetPerformanceStats.Update()
	perfStats := skynetPerformanceStats.Copy()
	skynetPerformanceStatsMu.Unlock()

	// Grab the siad uptime
	uptime := time.Since(api.StartTime()).Seconds()

	// Get the registry stats.
	renterPerf, err := api.renter.Performance()
	if err != nil {
		WriteError(w, Error{"unable to get renter registry status: " + err.Error()}, http.StatusBadRequest)
		return
	}

	// Check for any critical alerts.
	critAlerts := make([]modules.Alert, 0, 6)
	if api.gateway != nil {
		c, _, _ := api.gateway.Alerts()
		critAlerts = append(critAlerts, c...)
	}
	if api.cs != nil {
		c, _, _ := api.cs.Alerts()
		critAlerts = append(critAlerts, c...)
	}
	if api.tpool != nil {
		c, _, _ := api.tpool.Alerts()
		critAlerts = append(critAlerts, c...)
	}
	if api.wallet != nil {
		c, _, _ := api.wallet.Alerts()
		critAlerts = append(critAlerts, c...)
	}
	if api.renter != nil {
		c, _, _ := api.renter.Alerts()
		critAlerts = append(critAlerts, c...)
	}
	if api.host != nil {
		c, _, _ := api.host.Alerts()
		critAlerts = append(critAlerts, c...)
	}

	// Determine the wallet status.
	var walletStatus string
	var allowance skymodules.Allowance
	unlocked, err := api.wallet.Unlocked()
	if err != nil {
		WriteError(w, Error{"unable to get wallet lock status: " + err.Error()}, http.StatusBadRequest)
		return
	}
	walletFunds, _, _, err := api.wallet.ConfirmedBalance()
	if err != nil {
		WriteError(w, Error{"unable to get wallet balance: " + err.Error()}, http.StatusBadRequest)
		return
	}
	renterSettings, err := api.renter.Settings()
	if err != nil {
		WriteError(w, Error{"unable to get renter settings: " + err.Error()}, http.StatusBadRequest)
		return
	}
	allowance = renterSettings.Allowance
	if !unlocked {
		walletStatus = "locked"
	} else if walletFunds.Cmp(allowance.Funds.Div64(3)) < 0 {
		walletStatus = "low"
	} else if walletFunds.Cmp(allowance.Funds.Mul64(3)) > 0 {
		walletStatus = "high"
	} else {
		walletStatus = "healthy"
	}

	// Determine the allowance status.
	financialMetrics, err := api.renter.PeriodSpending()
	if err != nil {
		WriteError(w, Error{"unable to get renter financial breakdonw: " + err.Error()}, http.StatusBadRequest)
		return
	}
	_, _, unspentUnallocated := financialMetrics.SpendingBreakdown()
	var allowanceStatus string
	if unspentUnallocated.Cmp(types.NewCurrency64(10e3)) < 0 {
		allowanceStatus = "low"
	} else if unspentUnallocated.Cmp(allowance.Funds.Div64(5)) < 0 {
		allowanceStatus = "low"
	} else if unspentUnallocated.Cmp(allowance.Funds.Mul64(3).Div64(4)) > 0 && allowance.Funds.Cmp(types.NewCurrency64(50e3)) > 0 {
		allowanceStatus = "high"
	} else {
		allowanceStatus = "healthy"
	}

	// Get information about the total contracts size.
	var totalStorage uint64
	for _, c := range api.renter.Contracts() {
		totalStorage += c.Size()
	}

	WriteJSON(w, &SkynetStatsGET{
		NumCritAlerts: len(critAlerts),

		PerformanceStats: perfStats,

		BaseSectorUpload15mDataPoints: renterPerf.BaseSectorUploadStats.TotalDataPoints[0],
		BaseSectorUpload15mP99ms:      float64(renterPerf.BaseSectorUploadStats.Nines[0][1]) / float64(time.Millisecond),
		BaseSectorUpload15mP999ms:     float64(renterPerf.BaseSectorUploadStats.Nines[0][2]) / float64(time.Millisecond),
		BaseSectorUpload15mP9999ms:    float64(renterPerf.BaseSectorUploadStats.Nines[0][3]) / float64(time.Millisecond),

		ChunkUpload15mDataPoints: renterPerf.ChunkUploadStats.TotalDataPoints[0],
		ChunkUpload15mP99ms:      float64(renterPerf.ChunkUploadStats.Nines[0][1]) / float64(time.Millisecond),
		ChunkUpload15mP999ms:     float64(renterPerf.ChunkUploadStats.Nines[0][2]) / float64(time.Millisecond),
		ChunkUpload15mP9999ms:    float64(renterPerf.ChunkUploadStats.Nines[0][3]) / float64(time.Millisecond),

		RegistryRead15mDataPoints: renterPerf.RegistryReadStats.TotalDataPoints[0],
		RegistryRead15mP99ms:      float64(renterPerf.RegistryReadStats.Nines[0][1]) / float64(time.Millisecond),
		RegistryRead15mP999ms:     float64(renterPerf.RegistryReadStats.Nines[0][2]) / float64(time.Millisecond),
		RegistryRead15mP9999ms:    float64(renterPerf.RegistryReadStats.Nines[0][3]) / float64(time.Millisecond),

		RegistryWrite15mDataPoints: renterPerf.RegistryWriteStats.TotalDataPoints[0],
		RegistryWrite15mP99ms:      float64(renterPerf.RegistryWriteStats.Nines[0][1]) / float64(time.Millisecond),
		RegistryWrite15mP999ms:     float64(renterPerf.RegistryWriteStats.Nines[0][2]) / float64(time.Millisecond),
		RegistryWrite15mP9999ms:    float64(renterPerf.RegistryWriteStats.Nines[0][3]) / float64(time.Millisecond),

		SystemHealthScanDurationHours: float64(renterPerf.SystemHealthScanDuration) / float64(time.Hour),

		AllowanceStatus: allowanceStatus,
		ContractStorage: totalStorage,
		MaxStoragePrice: allowance.MaxStoragePrice.Mul(modules.BlockBytesPerMonthTerabyte).HumanString(),
		Repair:          rootDir.AggregateRepairSize,
		Storage:         rootDir.AggregateSize,
		StuckChunks:     rootDir.AggregateNumStuckChunks,
		WalletStatus:    walletStatus,

		Uptime:      int64(uptime),
		UploadStats: stats,
		VersionInfo: SkynetVersion{
			Version:     version,
			GitRevision: build.GitRevision,
		},
	})
}

// skykeyHandlerGET handles the API call to get a Skykey and its ID using its
// name or ID.
func (api *API) skykeyHandlerGET(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	// Parse Skykey id and name.
	name := req.FormValue("name")
	idString := req.FormValue("id")

	if idString == "" && name == "" {
		WriteError(w, Error{"you must specify the name or ID of the skykey"}, http.StatusInternalServerError)
		return
	}
	if idString != "" && name != "" {
		WriteError(w, Error{"you must specify either the name or ID of the skykey, not both"}, http.StatusInternalServerError)
		return
	}

	var sk skykey.Skykey
	var err error
	if name != "" {
		sk, err = api.renter.SkykeyByName(name)
	} else if idString != "" {
		var id skykey.SkykeyID
		err = id.FromString(idString)
		if err != nil {
			WriteError(w, Error{"failed to decode ID string: "}, http.StatusInternalServerError)
			return
		}
		sk, err = api.renter.SkykeyByID(id)
	}
	if err != nil {
		WriteError(w, Error{"failed to retrieve skykey: " + err.Error()}, http.StatusInternalServerError)
		return
	}

	skString, err := sk.ToString()
	if err != nil {
		WriteError(w, Error{"failed to decode skykey: " + err.Error()}, http.StatusInternalServerError)
		return
	}
	WriteJSON(w, SkykeyGET{
		Skykey: skString,
		Name:   sk.Name,
		ID:     sk.ID().ToString(),
		Type:   sk.Type.ToString(),
	})
}

// skykeyDeleteHandlerGET handles the API call to delete a Skykey using its name
// or ID.
func (api *API) skykeyDeleteHandlerPOST(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	// Parse Skykey id and name.
	name := req.FormValue("name")
	idString := req.FormValue("id")

	if idString == "" && name == "" {
		WriteError(w, Error{"you must specify the name or ID of the skykey"}, http.StatusBadRequest)
		return
	}
	if idString != "" && name != "" {
		WriteError(w, Error{"you must specify either the name or ID of the skykey, not both"}, http.StatusBadRequest)
		return
	}

	var err error
	if name != "" {
		err = api.renter.DeleteSkykeyByName(name)
	} else if idString != "" {
		var id skykey.SkykeyID
		err = id.FromString(idString)
		if err != nil {
			WriteError(w, Error{"Invalid skykey ID: " + err.Error()}, http.StatusBadRequest)
			return
		}
		err = api.renter.DeleteSkykeyByID(id)
	}

	if err != nil {
		WriteError(w, Error{"failed to delete skykey: " + err.Error()}, http.StatusInternalServerError)
		return
	}

	WriteSuccess(w)
}

// skykeyCreateKeyHandlerPost handles the API call to create a skykey using the renter's
// skykey manager.
func (api *API) skykeyCreateKeyHandlerPOST(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	// Parse skykey name and type
	name := req.FormValue("name")
	skykeyTypeString := req.FormValue("type")

	if name == "" {
		WriteError(w, Error{"you must specify the name of the skykey"}, http.StatusInternalServerError)
		return
	}

	if skykeyTypeString == "" {
		WriteError(w, Error{"you must specify the type of the skykey"}, http.StatusInternalServerError)
		return
	}

	var skykeyType skykey.SkykeyType
	err := skykeyType.FromString(skykeyTypeString)
	if err != nil {
		WriteError(w, Error{"failed to decode skykey type: " + err.Error()}, http.StatusInternalServerError)
		return
	}

	sk, err := api.renter.CreateSkykey(name, skykeyType)
	if err != nil {
		WriteError(w, Error{"failed to create skykey: " + err.Error()}, http.StatusInternalServerError)
		return
	}

	keyString, err := sk.ToString()
	if err != nil {
		WriteError(w, Error{"failed to decode skykey: " + err.Error()}, http.StatusInternalServerError)
		return
	}

	WriteJSON(w, SkykeyGET{
		Skykey: keyString,
		Name:   name,
		ID:     sk.ID().ToString(),
		Type:   skykeyTypeString,
	})
}

// skykeyAddKeyHandlerPost handles the API call to add a skykey to the renter's
// skykey manager.
func (api *API) skykeyAddKeyHandlerPOST(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	// Parse skykey.
	skString := req.FormValue("skykey")
	if skString == "" {
		WriteError(w, Error{"you must specify the name of the skykey"}, http.StatusInternalServerError)
		return
	}

	var sk skykey.Skykey
	err := sk.FromString(skString)
	if err != nil {
		WriteError(w, Error{"failed to decode skykey: " + err.Error()}, http.StatusInternalServerError)
		return
	}

	err = api.renter.AddSkykey(sk)
	if err != nil {
		WriteError(w, Error{"failed to add skykey: " + err.Error()}, http.StatusInternalServerError)
		return
	}

	WriteSuccess(w)
}

// skykeysHandlerGET handles the API call to get all of the renter's skykeys.
func (api *API) skykeysHandlerGET(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	skykeys, err := api.renter.Skykeys()
	if err != nil {
		WriteError(w, Error{"Unable to get skykeys: " + err.Error()}, http.StatusInternalServerError)
		return
	}

	res := SkykeysGET{
		Skykeys: make([]SkykeyGET, len(skykeys)),
	}
	for i, sk := range skykeys {
		skStr, err := sk.ToString()
		if err != nil {
			WriteError(w, Error{"failed to write skykey string: " + err.Error()}, http.StatusInternalServerError)
			return
		}
		res.Skykeys[i] = SkykeyGET{
			Skykey: skStr,
			Name:   sk.Name,
			ID:     sk.ID().ToString(),
			Type:   sk.Type.ToString(),
		}
	}
	WriteJSON(w, res)
}

// registryHandlerPOST handles the POST calls to /skynet/registry.
func (api *API) registryHandlerPOST(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	startTime := time.Now()

	// Decode request.
	dec := json.NewDecoder(req.Body)
	var rhp RegistryHandlerRequestPOST
	err := dec.Decode(&rhp)
	if err != nil {
		WriteError(w, Error{"Failed to decode request: " + err.Error()}, http.StatusBadRequest)
		return
	}

	// Check data length here to be able to offer a better and faster error
	// message than when the hosts return it.
	if len(rhp.Data) > modules.RegistryDataSize {
		WriteError(w, Error{fmt.Sprintf("Registry data is too big: %v > %v", len(rhp.Data), modules.RegistryDataSize)}, http.StatusBadRequest)
		return
	}

	// Update the registry.
	srv := modules.NewSignedRegistryValue(rhp.DataKey, rhp.Data, rhp.Revision, rhp.Signature)
	err = api.renter.UpdateRegistry(rhp.PublicKey, srv, renter.DefaultRegistryUpdateTimeout)
	if err != nil {
		skynetPerformanceStatsMu.Lock()
		skynetPerformanceStats.RegistryWrite.AddRequest(0, 0)
		skynetPerformanceStatsMu.Unlock()
		WriteError(w, Error{"Unable to update the registry: " + err.Error()}, http.StatusBadRequest)
		return
	}

	// Update the registry write stats.
	skynetPerformanceStatsMu.Lock()
	skynetPerformanceStats.RegistryWrite.AddRequest(time.Since(startTime), 0)
	skynetPerformanceStatsMu.Unlock()
	WriteSuccess(w)
}

// registryHandlerGET handles the GET calls to /skynet/registry.
func (api *API) registryHandlerGET(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	// Grab a start time for the registry read stats.
	startTime := time.Now()

	// Parse public key
	var spk types.SiaPublicKey
	err := spk.LoadString(req.FormValue("publickey"))
	if err != nil {
		WriteError(w, Error{"Unable to parse publickey param: " + err.Error()}, http.StatusBadRequest)
		return
	}

	// Parse datakey.
	var dataKey crypto.Hash
	err = dataKey.LoadString(req.FormValue("datakey"))
	if err != nil {
		WriteError(w, Error{"Unable to decode dataKey param: " + err.Error()}, http.StatusBadRequest)
		return
	}

	// Parse the timeout.
	timeout := renter.MaxRegistryReadTimeout
	timeoutStr := req.FormValue("timeout")
	if timeoutStr != "" {
		timeoutInt, err := strconv.Atoi(timeoutStr)
		if err != nil {
			WriteError(w, Error{"unable to parse 'timeout' parameter: " + err.Error()}, http.StatusBadRequest)
			return
		}
		timeout = time.Duration(timeoutInt) * time.Second
		if timeout > renter.MaxRegistryReadTimeout || timeout == 0 {
			WriteError(w, Error{fmt.Sprintf("Invalid 'timeout' parameter, needs to be between 1s and %ds", renter.MaxRegistryReadTimeout)}, http.StatusBadRequest)
			return
		}
	}

	// Read registry.
	ctx, cancel := context.WithTimeout(req.Context(), timeout)
	defer cancel()
	srv, err := api.renter.ReadRegistry(ctx, spk, dataKey)
	if err != nil {
		handleSkynetError(w, "unable to read from the registry", err)
		return
	}

	// Update the registry read stats.
	skynetPerformanceStatsMu.Lock()
	skynetPerformanceStats.RegistryRead.AddRequest(time.Since(startTime), 0)
	skynetPerformanceStatsMu.Unlock()

	// Send response.
	WriteJSON(w, RegistryHandlerGET{
		Data:      hex.EncodeToString(srv.Data),
		Revision:  srv.Revision,
		Signature: hex.EncodeToString(srv.Signature[:]),
	})
}

// skynetRestoreHandlerPOST handles the POST calls to /skynet/restore.
func (api *API) skynetRestoreHandlerPOST(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	// Restore Skyfile
	skylink, err := api.renter.RestoreSkyfile(req.Body)
	if err != nil {
		WriteError(w, Error{"unable to restore skyfile: " + err.Error()}, http.StatusBadRequest)
		return
	}

	WriteJSON(w, SkynetRestorePOST{
		Skylink: skylink.String(),
	})
}

// skynetMetadataHandlerGET is the handler for the /skynet/metadata endpoint.
func (api *API) skynetMetadataHandlerGET(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
	// Parse the skylink from the raw URL of the request. Any special characters
	// in the raw URL are encoded, allowing us to differentiate e.g. the '?'
	// that begins query parameters from the encoded version '%3F'.
	skylink, _, _, err := parseSkylinkURL(req.URL.String(), "/skynet/metadata/")
	if err != nil {
		WriteError(w, Error{fmt.Sprintf("error parsing skylink: %v", err)}, http.StatusBadRequest)
		return
	}

	// Parse the query params.
	queryForm, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		WriteError(w, Error{"failed to parse query params"}, http.StatusBadRequest)
		return
	}

	// Parse the timeout.
	timeout := DefaultSkynetRequestTimeout
	timeoutStr := queryForm.Get("timeout")
	if timeoutStr != "" {
		timeoutInt, err := strconv.Atoi(timeoutStr)
		if err != nil {
			WriteError(w, Error{"unable to parse 'timeout' parameter: " + err.Error()}, http.StatusBadRequest)
			return
		}

		if timeoutInt > MaxSkynetRequestTimeout {
			WriteError(w, Error{fmt.Sprintf("'timeout' parameter too high, maximum allowed timeout is %ds", MaxSkynetRequestTimeout)}, http.StatusBadRequest)
			return
		}
		timeout = time.Duration(timeoutInt) * time.Second
	}

	// Parse pricePerMS.
	pricePerMS := DefaultSkynetPricePerMS
	pricePerMSStr := queryForm.Get("priceperms")
	if pricePerMSStr != "" {
		pricePerMSParsed, err := types.ParseCurrency(pricePerMSStr)
		if err != nil {
			WriteError(w, Error{"unable to parse 'pricePerMS' parameter: " + err.Error()}, http.StatusBadRequest)
			return
		}
		_, err = fmt.Sscan(pricePerMSParsed, &pricePerMS)
		if err != nil {
			WriteError(w, Error{"unable to parse 'pricePerMS' parameter: " + err.Error()}, http.StatusBadRequest)
			return
		}
	}

	// Fetch the skyfile's streamer to serve the basesector of the file
	streamer, err := api.renter.DownloadSkylinkBaseSector(skylink, timeout, pricePerMS)
	if err != nil {
		handleSkynetError(w, "failed to fetch base sector", err)
		return
	}
	defer func() {
		// At this point we have already responded so we can't write a potential
		// error here.
		_ = streamer.Close()
	}()

	// Read base sector.
	baseSector, err := ioutil.ReadAll(streamer)
	if err != nil {
		WriteError(w, Error{fmt.Sprintf("failed to read base sector: %v", err)}, http.StatusInternalServerError)
		return
	}

	// Parse it.
	_, _, _, rawMD, _, err := skymodules.ParseSkyfileMetadata(baseSector)
	if err != nil {
		WriteError(w, Error{fmt.Sprintf("failed to fetch skylink: %v", err)}, http.StatusInternalServerError)
		return
	}
	http.ServeContent(w, req, "", time.Time{}, bytes.NewReader(rawMD))
}

// skynetSkylinkUnpinHandlerPOST will unpin a skylink from this Sia node.
func (api *API) skynetSkylinkUnpinHandlerPOST(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
	strLink := ps.ByName("skylink")
	var skylink skymodules.Skylink
	err := skylink.LoadString(strLink)
	if err != nil {
		WriteError(w, Error{fmt.Sprintf("error parsing skylink: %v", err)}, http.StatusBadRequest)
		return
	}

	// Parse the query params.
	queryForm, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		WriteError(w, Error{fmt.Sprintf("failed to parse query params: %v", err)}, http.StatusBadRequest)
		return
	}

	// Parse the siaPath
	var siaPath skymodules.SiaPath
	siaPathStr := queryForm.Get("siapath")
	if siaPathStr != "" {
		// If a siaPath was provided, load it and also generate an extended
		// siaPath
		err = siaPath.LoadString(siaPathStr)
		if err != nil {
			WriteError(w, Error{fmt.Sprintf("failed to parse siapath: %v", err)}, http.StatusBadRequest)
			return
		}
		extendedSiaPath, err := siaPath.AddSuffixStr(skymodules.ExtendedSuffix)
		if err != nil {
			WriteError(w, Error{fmt.Sprintf("failed to create extended siapath: %v", err)}, http.StatusBadRequest)
			return
		}

		// Try and delete skyfile
		err = api.renter.DeleteFile(siaPath)
		if err != nil && !strings.Contains(err.Error(), filesystem.ErrNotExist.Error()) {
			WriteError(w, Error{fmt.Sprintf("failed to delete skyfile: %v", err)}, http.StatusBadRequest)
			return
		}

		// Try ad delete extended skyfile
		err = api.renter.DeleteFile(extendedSiaPath)
		if err != nil && !strings.Contains(err.Error(), filesystem.ErrNotExist.Error()) {
			WriteError(w, Error{fmt.Sprintf("failed to delete extended skyfile: %v", err)}, http.StatusBadRequest)
			return
		}
	}

	// Unpin the Skylink
	err = api.renter.UnpinSkylink(skylink)
	if err != nil {
		handleSkynetError(w, "failed to unpin skylink", err)
		return
	}
	WriteSuccess(w)
}
