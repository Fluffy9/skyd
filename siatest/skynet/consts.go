package skynet

import (
	"os"

	"gitlab.com/SkynetLabs/skyd/siatest"
	"go.sia.tech/siad/persist"
)

// skynetTestDir creates a temporary testing directory for a skynet test. This
// should only every be called once per test. Otherwise it will delete the
// directory again.
func skynetTestDir(testName string) string {
	path := siatest.TestDir("skynet", testName)
	if err := os.MkdirAll(path, persist.DefaultDiskPermissionsTest); err != nil {
		panic(err)
	}
	return path
}
