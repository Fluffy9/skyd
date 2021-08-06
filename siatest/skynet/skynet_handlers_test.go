package skynet

import (
	"net/http"
	"regexp"
	"testing"

	"os"
	"path/filepath"
	"strings"

	"gitlab.com/NebulousLabs/errors"
	"gitlab.com/NebulousLabs/fastrand"
	"gitlab.com/SkynetLabs/skyd/node"
	"gitlab.com/SkynetLabs/skyd/node/api/client"
	"gitlab.com/SkynetLabs/skyd/siatest"
	"gitlab.com/SkynetLabs/skyd/siatest/dependencies"
	"gitlab.com/SkynetLabs/skyd/skymodules"
	"go.sia.tech/siad/persist"
)

// TestSkynetSkylinkHandlerGET tests the behaviour of SkynetSkylinkHandlerGET
// when it handles different combinations of metadata and content. These tests
// use the fixtures in `testdata/skylink_fixtures.json`.
func TestSkynetSkylinkHandlerGET(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	t.Parallel()

	// Create a testgroup.
	groupParams := siatest.GroupParams{
		Hosts:  3,
		Miners: 1,
	}
	testDir := siatest.TestDir("renter", t.Name())
	if err := os.MkdirAll(testDir, persist.DefaultDiskPermissionsTest); err != nil {
		t.Fatal(err)
	}
	tg, err := siatest.NewGroupFromTemplate(testDir, groupParams)
	if err != nil {
		t.Fatal("Failed to create group: ", err)
	}
	defer func() {
		if err := tg.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	// Add a Renter node.
	renterParams := node.Renter(filepath.Join(testDir, "renter"))
	renterParams.RenterDeps = &dependencies.DependencyResolveSkylinkToFixture{}
	nodes, err := tg.AddNodes(renterParams)
	if err != nil {
		t.Fatal(err)
	}
	r := nodes[0]
	defer func() { _ = tg.RemoveNode(r) }()

	subTests := []struct {
		Name             string
		Skylink          string
		ExpectedError    string
		ExpectedRedirect string
	}{
		{
			// ValidSkyfile is the happy path, ensuring that we don't get errors
			// on valid data.
			Name:          "ValidSkyfileWithTrailingSlash",
			Skylink:       "_A6d-2CpM2OQ-7m5NPAYW830NdzC3wGydFzzd-KnHXhwJA/",
			ExpectedError: "",
		},
		{
			// ValidSkyfile is the happy path, ensuring that we don't get errors
			// on valid data.
			Name:             "ValidSkyfileNoTrailingSlash",
			Skylink:          "_A6d-2CpM2OQ-7m5NPAYW830NdzC3wGydFzzd-KnHXhwJA",
			ExpectedError:    "Redirect",
			ExpectedRedirect: "_A6d-2CpM2OQ-7m5NPAYW830NdzC3wGydFzzd-KnHXhwJA/",
		},
		{
			// SingleFileDefaultPath ensures that we return an error if a single
			// file has a `defaultpath` field.
			Name:          "SingleFileDefaultPath",
			Skylink:       "3AAcCO73xMbehYaK7bjDGCtW0GwOL6Swl-lNY52Pb_APzA",
			ExpectedError: "defaultpath is not allowed on single files",
		},
		{
			// DefaultPathDisableDefaultPath ensures that we return an error if
			// a file has both defaultPath and disableDefaultPath set.
			Name:          "DefaultPathDisableDefaultPath",
			Skylink:       "3BBcCO73xMbehYaK7bjDGCtW0GwOL6Swl-lNY52Pb_APzA",
			ExpectedError: "both defaultpath and disabledefaultpath are set",
		},
		{
			// NonRootDefaultPath ensures that we return an error if a file has
			// a non-root defaultPath.
			Name:          "NonRootDefaultPath",
			Skylink:       "4BBcCO73xMbehYaK7bjDGCtW0GwOL6Swl-lNY52Pb_APzA",
			ExpectedError: "which refers to a non-root file",
		},
		{
			// NonRootPath ensures that we can get a non-root file by passing
			// its path manually.
			Name:          "NonRootPath",
			Skylink:       "4BBcCO73xMbehYaK7bjDGCtW0GwOL6Swl-lNY52Pb_APzA/dir/file.txt",
			ExpectedError: "",
		},
		{
			// DetectRedirect ensures that if the skylink doesn't have a
			// trailing slash and has a default path that results in an HTML
			// file we redirect to the same skylink with a trailing slash.
			Name:             "DetectRedirect",
			Skylink:          "4CCcCO73xMbehYaK7bjDGCtW0GwOL6Swl-lNY52Pb_APzA?foo=bar",
			ExpectedError:    "Redirect",
			ExpectedRedirect: "4CCcCO73xMbehYaK7bjDGCtW0GwOL6Swl-lNY52Pb_APzA/?foo=bar",
		},
		{
			// DetectRedirectNoQuestionMark ensures that if the skylink doesn't
			// have a trailing slash, and doesn't have query params, and has a
			// default path that results in an HTML file we redirect to the same
			// skylink with a trailing slash and without a question mark.
			Name:             "DetectRedirectNoQuestionMark",
			Skylink:          "4CCcCO73xMbehYaK7bjDGCtW0GwOL6Swl-lNY52Pb_APzA?",
			ExpectedError:    "Redirect",
			ExpectedRedirect: "4CCcCO73xMbehYaK7bjDGCtW0GwOL6Swl-lNY52Pb_APzA/",
		},
		{
			// DetectRedirectWithEncoding ensures that if the skylink needs to
			// be redirected and has encoded special characters in its URL, that
			// these are not decoded by redirecting.
			Name:             "DetectRedirectWithEncoding",
			Skylink:          "4CCcCO73xMbehYaK7bjDGCtW0GwOL6Swl-lNY52Pb_APzA?filename=encoding%23test%3F",
			ExpectedError:    "Redirect",
			ExpectedRedirect: "4CCcCO73xMbehYaK7bjDGCtW0GwOL6Swl-lNY52Pb_APzA/?filename=encoding%23test%3F",
		},
		{
			// PartialFilenameWithEncoding ensures that if a partial version of
			// an existing path has encoded special characters in its URL, no
			// file found.
			Name:          "PartialFilenameWithEncoding",
			Skylink:       "4CCcCO73xMbehYaK7bjDGCtW0GwOL6Swl-lNY52Pb_APzA/test%3F",
			ExpectedError: "failed to download contents for path: /test?",
		},
		{
			// FilenameWithEncoding ensures that if the path has encoded special
			// characters in its URL, that the correct file is found.
			Name:          "FilenameWithEncoding",
			Skylink:       "4CCcCO73xMbehYaK7bjDGCtW0GwOL6Swl-lNY52Pb_APzA/test%3Fencoding",
			ExpectedError: "",
		},
		{
			// EnsureNoRedirect ensures that there is no redirect if the skylink
			// has a trailing slash.
			// This is the happy case for DetectRedirect.
			Name:          "EnsureNoRedirect",
			Skylink:       "4CCcCO73xMbehYaK7bjDGCtW0GwOL6Swl-lNY52Pb_APzA/",
			ExpectedError: "",
		},
		{
			// IncompletePath ensures that we return an error if a partial
			// fragment of a path is passed.
			Name:          "IncompletePath",
			Skylink:       "4CCcCO73xMbehYaK7bjDGCtW0GwOL6Swl-lNY52Pb_APzA/di",
			ExpectedError: "failed to download contents for path: /di",
		},
		{
			// CompletePath ensures that we don't get an error on a complete,
			// existent dir path.
			Name:          "CompletePath",
			Skylink:       "4CCcCO73xMbehYaK7bjDGCtW0GwOL6Swl-lNY52Pb_APzA/dir",
			ExpectedError: "",
		},
	}

	r = tg.Renters()[0]
	r.Client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return errors.New("Redirect:###" + req.URL.String() + "###")
	}
	re := regexp.MustCompile(`Redirect:###(.*)###`)
	// Run the tests.
	for _, test := range subTests {
		_, err = r.SkynetSkylinkGet(test.Skylink)
		if err == nil && test.ExpectedError != "" {
			t.Fatalf("%s failed: expected error '%s', got '%+v'\n", test.Name, test.ExpectedError, err)
		}
		if err != nil && (test.ExpectedError == "" || !strings.Contains(err.Error(), test.ExpectedError)) {
			t.Fatalf("%s failed: expected error '%s', got '%+v'\n", test.Name, test.ExpectedError, err)
		}
		// Add a specific check for the redirect URL.
		if err != nil && test.ExpectedError == "Redirect" {
			matches := re.FindStringSubmatch(err.Error())
			if len(matches) < 2 {
				t.Fatalf("%s failed: redirect string not found. Error str: %s\n", test.Name, err.Error())
			}
			// We are using HasSuffix instead of a direct match because the URL
			// to which we get redirected will have some mock server prefix
			// similar to `http://[::]:51866/skynet/skylink/`.
			if !strings.HasSuffix(matches[1], test.ExpectedRedirect) {
				t.Fatalf("%s failed: expected redirect '%s', got '%s'\n", test.Name, test.ExpectedRedirect, matches[1])
			}
		}
	}
}

// TestSkynetSkylinkPinHandlerPOST ensures various aspects of the correct
// functioning of the skynetSkylinkPinHandlerPOST method.
func TestSkynetSkylinkPinHandlerPOST(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	t.Parallel()

	// Create a test group.
	groupParams := siatest.GroupParams{
		Hosts:   3,
		Miners:  1,
		Portals: 1,
	}
	groupDir := skynetTestDir(t.Name())

	// Specify subtests to run
	subTests := []siatest.SubTest{
		{Name: "EnsureSkynetSkylinkHeader", Test: testEnsureSkynetSkylinkHeader},
	}

	// Run tests
	if err := siatest.RunSubTests(t, groupParams, groupDir, subTests); err != nil {
		t.Fatal(err)
	}
}

// testEnsureSkynetSkylinkHeader tests the skynetSkylinkPinHandlerPOST method
// returns a Skynet-Skylink header.
func testEnsureSkynetSkylinkHeader(t *testing.T, tg *siatest.TestGroup) {
	r := tg.Renters()[0]

	// Upload a test file.
	skylink, _, _, err := r.UploadNewSkyfileWithDataBlocking(t.Name(), fastrand.Bytes(100), false)
	if err != nil {
		t.Fatal("failed to upload test data", err)
	}

	// Re-pin the test file.
	// Use an unsafe client to get access to the response headers.
	uc := client.NewUnsafeClient(r.Client)
	spp := skymodules.SkyfilePinParameters{
		SiaPath: skymodules.RandomSiaPath(),
	}
	header, _, err := uc.SkynetSkylinkPinPostRawResponse(skylink, spp)
	if err != nil {
		t.Fatal(err)
	}

	// Verify the response header contains the same Skylink.
	if header.Get("Skynet-Skylink") != skylink {
		t.Fatal("unexpected")
	}
}
