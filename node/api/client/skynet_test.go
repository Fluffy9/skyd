package client

import (
	"net/url"
	"os"
	"testing"

	"gitlab.com/NebulousLabs/fastrand"
	"gitlab.com/SkynetLabs/skyd/skykey"
	"gitlab.com/SkynetLabs/skyd/skymodules"
	"go.sia.tech/siad/types"
)

// TestUrlValuesFromSkynetUploadParams is a unit test that covers the helper
// functions transforming upload parameters into url values.
func TestUrlValuesFromSkynetUploadParams(t *testing.T) {
	t.Parallel()

	// hasValueForKeys is a small helper function that checks whether the given
	// url.Values contains a value for the given list of expected keys.
	hasValueForKeys := func(values url.Values, keys []string) bool {
		for _, key := range keys {
			if values.Get(key) == "" {
				return false
			}
		}
		return true
	}

	// Create monetization.
	monetization := &skymodules.Monetization{
		Monetizers: []skymodules.Monetizer{
			{
				Address:  types.UnlockHash{},
				Amount:   types.NewCurrency64(fastrand.Uint64n(1000) + 1),
				Currency: skymodules.CurrencyUSD,
			},
		},
	}
	fastrand.Read(monetization.Monetizers[0].Address[:])

	// Create SkyfileMultipartUploadParameters.
	smup := skymodules.SkyfileMultipartUploadParameters{
		SiaPath:             skymodules.RandomSiaPath(),
		Force:               true,
		Root:                true,
		BaseChunkRedundancy: 2,
		Filename:            "file.txt",
		DefaultPath:         "index.html",
		DisableDefaultPath:  false,
		Monetization:        monetization,
	}

	// Verify 'urlValuesFromSkyfileMultipartUploadParameters' helper
	values, err := urlValuesFromSkyfileMultipartUploadParameters(smup)
	if err != nil {
		t.Fatal(err)
	}
	if !hasValueForKeys(values, []string{
		"siapath",
		"force",
		"root",
		"basechunkredundancy",
		"filename",
		"monetization",
	}) {
		t.Fatal("unexpected")
	}

	// Create SkyfilePinParameters.
	spp := skymodules.SkyfilePinParameters{
		SiaPath:             skymodules.RandomSiaPath(),
		Force:               true,
		Root:                true,
		BaseChunkRedundancy: 2,
	}

	// Verify 'urlValuesFromSkyfilePinParameters' helper
	values = urlValuesFromSkyfilePinParameters(spp)
	if !hasValueForKeys(values, []string{
		"siapath",
		"force",
		"root",
		"basechunkredundancy",
	}) {
		t.Fatal("unexpected")
	}

	// Create SkyfileUploadParameters.
	var skyKeyID skykey.SkykeyID
	fastrand.Read(skyKeyID[:])

	sup := skymodules.SkyfileUploadParameters{
		SiaPath:             skymodules.RandomSiaPath(),
		DryRun:              true,
		Force:               true,
		Root:                true,
		BaseChunkRedundancy: 2,
		Filename:            "file.txt",
		Mode:                os.FileMode(0644),
		DefaultPath:         "index.html",
		DisableDefaultPath:  false,
		Monetization:        monetization,
		SkykeyName:          "somename",
		SkykeyID:            skyKeyID,
	}

	// Verify 'urlValuesFromSkyfileMultipartUploadParameters' helper
	values, err = urlValuesFromSkyfileUploadParameters(sup)
	if err != nil {
		t.Fatal(err)
	}
	if !hasValueForKeys(values, []string{
		"siapath",
		"dryrun",
		"force",
		"root",
		"basechunkredundancy",
		"filename",
		"mode",
		"monetization",
		"skykeyname",
		"skykeyid",
	}) {
		t.Fatal("unexpected")
	}
}
