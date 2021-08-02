package skynet

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/eventials/go-tus"
	"gitlab.com/NebulousLabs/errors"
	"gitlab.com/NebulousLabs/fastrand"
	"gitlab.com/SkynetLabs/skyd/node"
	"gitlab.com/SkynetLabs/skyd/node/api/client"
	"gitlab.com/SkynetLabs/skyd/siatest"
	"gitlab.com/SkynetLabs/skyd/siatest/dependencies"
	"gitlab.com/SkynetLabs/skyd/skymodules"
	"gitlab.com/SkynetLabs/skyd/skymodules/renter"
	"go.sia.tech/siad/crypto"
	"go.sia.tech/siad/modules"
)

// TestSkynetTUSUploader runs all skynetTUSUploader related tests.
func TestSkynetTUSUploader(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	t.Parallel()

	// Prepare a testgroup.
	groupParams := siatest.GroupParams{
		Hosts:   3,
		Miners:  1,
		Renters: 1,
	}
	groupDir := skynetTestDir(t.Name())
	tg, err := siatest.NewGroupFromTemplate(groupDir, groupParams)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := tg.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	// Run tests.
	t.Run("Basic", func(t *testing.T) {
		testTUSUploaderBasic(t, tg.Renters()[0])
	})
	t.Run("Options", func(t *testing.T) {
		testOptionsHandler(t, tg.Renters()[0])
	})
	t.Run("TooLarge", func(t *testing.T) {
		testTUSUploaderTooLarge(t, tg.Renters()[0])
	})
	t.Run("PruneIdle", func(t *testing.T) {
		testTUSUploaderPruneIdle(t, tg.Renters()[0])
	})
	t.Run("UnstableConnection", func(t *testing.T) {
		testTUSUploaderUnstableConnection(t, tg)
	})
	t.Run("DroppedConnection", func(t *testing.T) {
		testTUSUploaderConnectionDropped(t, tg)
	})
}

// testTUSUploadBasic tests uploading multiple files using the TUS protocol and
// verifies that pruning doesn't delete completed .sia files.
func testTUSUploaderBasic(t *testing.T, r *siatest.TestNode) {
	// Get the number of files before the test.
	dir, err := r.RenterDirRootGet(skymodules.SkynetFolder)
	if err != nil {
		t.Fatal(err)
	}
	nFilesBefore := dir.Directories[0].AggregateNumFiles

	// Declare the chunkSize.
	chunkSize := 2 * int64(skymodules.ChunkSize(crypto.TypePlain, uint64(skymodules.RenterDefaultDataPieces)))

	// Declare a test helper that uploads a file and downloads it.
	uploadTest := func(fileSize int64) error {
		uploadedData := fastrand.Bytes(int(fileSize))
		fileName := hex.EncodeToString(fastrand.Bytes(10))
		fileType := hex.EncodeToString(fastrand.Bytes(10))
		skylink, err := r.SkynetTUSUploadFromBytes(uploadedData, chunkSize, fileName, fileType)
		if err != nil {
			return err
		}

		// Download the uploaded data and compare it to the uploaded data.
		downloadedData, err := r.SkynetSkylinkGet(skylink)
		if err != nil {
			return err
		}
		_, sm, err := r.SkynetMetadataGet(skylink)
		if err != nil {
			return err
		}
		if !bytes.Equal(uploadedData, downloadedData) {
			return errors.New("data doesn't match")
		}
		if sm.Length != uint64(len(uploadedData)) {
			return errors.New("wrong length in metadata")
		}
		if sm.Filename != fileName {
			t.Fatalf("Invalid filename %v != %v", sm.Filename, fileName)
		}
		if len(sm.Subfiles) != 1 {
			t.Fatal("expected one subfile but got", len(sm.Subfiles))
		}
		ssm, exists := sm.Subfiles[fileName]
		if !exists {
			t.Fatal("subfile missing")
		}
		if ssm.Filename != sm.Filename {
			t.Fatal("filename mismatch")
		}
		if ssm.Len != sm.Length {
			t.Fatal("length mismatch")
		}
		if ssm.Offset != 0 {
			t.Fatal("offset should be zero")
		}
		if ssm.ContentType != fileType {
			t.Fatalf("wrong content-type %v != %v", ssm.ContentType, fileType)
		}
		return nil
	}

	// Upload a large file.
	if err := uploadTest(chunkSize*5 + chunkSize/2); err != nil {
		t.Fatal(err)
	}

	// Upload a byte that's smaller than a sector but still a large file.
	if err := uploadTest(int64(modules.SectorSize) - 1); err != nil {
		t.Fatal(err)
	}

	// Upload a small file.
	if err := uploadTest(1); err != nil {
		t.Fatal(err)
	}

	// Upload empty file.
	if err := uploadTest(0); err != nil {
		t.Fatal(err)
	}

	// Set the max size to 1 chunkSize.
	err = os.Setenv("TUS_MAXSIZE", fmt.Sprint(chunkSize))
	if err != nil {
		t.Fatal(err)
	}

	// Restart the renter for the change to take effect.
	err = r.RestartNode()
	if err != nil {
		t.Fatal(err)
	}

	// Upload file that is too large.
	if err := uploadTest(2 * chunkSize); err == nil || !strings.Contains(err.Error(), "upload body is to large") {
		t.Fatal(err)
	}

	// Reset size.
	err = os.Unsetenv("TUS_MAXSIZE")
	if err != nil {
		t.Fatal(err)
	}

	// Restart the renter again.
	err = r.RestartNode()
	if err != nil {
		t.Fatal(err)
	}

	// Wait for two full pruning intervals to make sure pruning ran at least
	// once.
	time.Sleep(2 * renter.PruneTUSUploadTimeout)

	// Check that the number of files increased by 6. One for the small and zero
	// uploads and 2 for each of the large ones.
	dir, err = r.RenterDirRootGet(skymodules.SkynetFolder)
	if err != nil {
		t.Fatal(err)
	}
	nFiles := dir.Directories[0].AggregateNumFiles
	if nFiles-nFilesBefore != 6 {
		t.Fatal("expected 6 new files but got", nFiles-nFilesBefore)
	}
}

// testTUSUploaderTooLarge tests the user specified max size of the TUS
// endpoints.
func testTUSUploaderTooLarge(t *testing.T, r *siatest.TestNode) {
	// Declare the chunkSize and data.
	chunkSize := 2 * int64(skymodules.ChunkSize(crypto.TypePlain, uint64(skymodules.RenterDefaultDataPieces)))
	data := fastrand.Bytes(int(chunkSize))

	// Upload with a max size that equals the uploaded data. This should work.
	_, err := r.SkynetTUSUploadFromBytesWithMaxSize(data, chunkSize, "success", "", int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}

	// Upload with a max size that is 1 byte smaller than the file's size. This should fail.
	_, err = r.SkynetTUSUploadFromBytesWithMaxSize(data, chunkSize, "failure", "", int64(len(data))-1)
	if err == nil {
		t.Fatal(err)
	}
}

// testOptionsHandler makes sure that the tus endpoints set the expected header
// when requesting them with the OPTIONS request type.
func testOptionsHandler(t *testing.T, r *siatest.TestNode) {
	testEndpoint := func(url string) {
		req, err := http.NewRequest("OPTIONS", url, nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		if err := resp.Body.Close(); err != nil {
			t.Fatal(err)
		}
		if _, ok := resp.Header["Tus-Extension"]; !ok {
			t.Fatal("missing header")
		}
		if _, ok := resp.Header["Tus-Resumable"]; !ok {
			t.Fatal("missing header")
		}
		if _, ok := resp.Header["Tus-Version"]; !ok {
			t.Fatal("missing header")
		}
	}

	// Test /skynet/tus
	testEndpoint(fmt.Sprintf("http://%s/skynet/tus", r.APIAddress()))

	// Create an uploader to get an upload id.
	chunkSize := 2 * int64(skymodules.ChunkSize(crypto.TypePlain, uint64(skymodules.RenterDefaultDataPieces)))
	fileSize := chunkSize*5 + chunkSize/2 // 5 1/2 chunks.
	uploadedData := fastrand.Bytes(int(fileSize))
	tc, upload, err := r.SkynetTUSNewUploadFromBytes(uploadedData, chunkSize)
	if err != nil {
		t.Fatal(err)
	}
	uploader, err := tc.CreateUpload(upload)
	if err != nil {
		t.Fatal(err)
	}

	// Test /skynet/tus/:id
	testEndpoint(uploader.Url())
}

// testTUSUploaderPruneIdle checks that incomplete uploads get pruned after a
// while and have their .sia files deleted from disk.
func testTUSUploaderPruneIdle(t *testing.T, r *siatest.TestNode) {
	// Get the number of files before the test.
	dir, err := r.RenterDirRootGet(skymodules.SkynetFolder)
	if err != nil {
		t.Fatal(err)
	}
	nFilesBefore := dir.Directories[0].AggregateNumFiles

	// upload a 100 byte file in chunks of 10 bytes.
	chunkSize := 2 * int64(skymodules.ChunkSize(crypto.TypePlain, uint64(skymodules.RenterDefaultDataPieces)))
	fileSize := chunkSize*5 + chunkSize/2 // 5 1/2 chunks.
	uploadedData := fastrand.Bytes(int(fileSize))

	// Get a tus client and upload.
	tc, upload, err := r.SkynetTUSNewUploadFromBytes(uploadedData, chunkSize)
	if err != nil {
		t.Fatal(err)
	}

	// Start upload.
	uploader, err := tc.CreateUpload(upload)
	if err != nil {
		t.Fatal(err)
	}

	// Upload a single chunk.
	err = uploader.UploadChunck()
	if err != nil {
		t.Fatal(err)
	}

	// Wait for two full pruning intervals to make sure pruning ran at least
	// once.
	time.Sleep(2 * renter.PruneTUSUploadTimeout)

	// Upload another chunk.
	err = uploader.UploadChunck()
	if err == nil || !strings.Contains(err.Error(), "404") {
		t.Fatal(err)
	}

	// Try to resume upload.
	uploader, err = tc.ResumeUpload(upload)
	if err == nil || !errors.Contains(err, tus.ErrUploadNotFound) {
		t.Fatal(err)
	}

	// Check that the number of files didn't increase since the new files were
	// purged.
	dir, err = r.RenterDirRootGet(skymodules.SkynetFolder)
	if err != nil {
		t.Fatal(err)
	}
	nFiles := dir.Directories[0].AggregateNumFiles
	if nFiles-nFilesBefore != 0 {
		t.Fatal("expected 0 new files but got", nFiles-nFilesBefore)
	}
}

// testTUSUploaderUnstableConnection tests uploading with a TUS uploader where
// every chunk upload fails halfway through.
func testTUSUploaderUnstableConnection(t *testing.T, tg *siatest.TestGroup) {
	// Add a custom renter with dependency.
	rp := node.RenterTemplate
	rp.RenterDeps = &dependencies.DependencyUnstableTUSUpload{}
	nodes, err := tg.AddNodes(rp)
	if err != nil {
		t.Fatal(err)
	}
	r := nodes[0]
	defer func() {
		if err := tg.RemoveNode(r); err != nil {
			t.Fatal(err)
		}
	}()

	// Get a tus client.
	chunkSize := 2 * int64(skymodules.ChunkSize(crypto.TypePlain, uint64(skymodules.RenterDefaultDataPieces)))

	// Upload some chunks.
	uploadedData := fastrand.Bytes(int(chunkSize * 10))
	tc, upload, err := r.SkynetTUSNewUploadFromBytes(uploadedData, chunkSize)

	// Create uploader.
	uploader, err := tc.CreateUpload(upload)
	if err != nil {
		t.Fatal(err)
	}

	// Upload all the chunks. Whenever we encounter an error we resume the
	// upload until we run out of retries.
	remainingChunks := len(uploadedData) / int(chunkSize)
	remainingTries := 5 * remainingChunks
	for remainingChunks > 0 {
		err = uploader.UploadChunck()
		if err == nil {
			remainingChunks--
			continue // continue
		}
		// Decrement remaining tries.
		if remainingTries == 0 {
			t.Fatal("out of retries")
		}
		remainingTries--
		// Resume upload.
		uploader, err = tc.ResumeUpload(upload)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Fetch skylink after upload is done.
	skylink, err := client.SkylinkFromTUSURL(tc, uploader.Url())
	if err != nil {
		t.Fatal(err)
	}

	// Download the uploaded data and compare it to the uploaded data.
	downloadedData, err := r.SkynetSkylinkGet(skylink)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(uploadedData, downloadedData) {
		t.Fatal("data doesn't match")
	}
}

// testTUSUploaderConnectionDropped tests dropping the connection halfway
// through a chunk.
func testTUSUploaderConnectionDropped(t *testing.T, tg *siatest.TestGroup) {
	// Add a custom renter with dependency.
	rp := node.RenterTemplate
	deps := dependencies.NewDependencyTUSConnectionDrop()
	rp.RenterDeps = deps
	nodes, err := tg.AddNodes(rp)
	if err != nil {
		t.Fatal(err)
	}
	r := nodes[0]
	defer func() {
		if err := tg.RemoveNode(r); err != nil {
			t.Fatal(err)
		}
	}()

	// Get a tus client.
	chunkSize := int64(skymodules.ChunkSize(crypto.TypePlain, uint64(skymodules.RenterDefaultDataPieces)))

	// Create upload for a file that is 1.5 chunks large.
	uploadedData := fastrand.Bytes(int(3 * chunkSize / 2))
	tc, upload, err := r.SkynetTUSNewUploadFromBytes(uploadedData, chunkSize)

	// Create uploader and upload the first chunk.
	uploader, err := tc.CreateUpload(upload)
	if err != nil {
		t.Fatal(err)
	}
	err = uploader.UploadChunck()
	if err != nil {
		t.Fatal(err)
	}

	// Trigger the failure on the dependency and try to upload the remaining
	// data. That should fail.
	deps.Fail()
	err = uploader.Upload()
	if err == nil {
		t.Fatal("should fail")
	}

	// Pick up upload from where we left off. The offset should be at 1 chunk since
	// we only managed to upload 1 chunk successfully.
	uploader, err = tc.ResumeUpload(upload)
	if err != nil {
		t.Fatal(err)
	}
	if upload.Offset() != chunkSize {
		t.Fatal("wrong offset")
	}
	// Upload remaining data. Should work now.
	err = uploader.Upload()
	if err != nil {
		t.Fatal(err)
	}

	// Fetch skylink after upload is done.
	skylink, err := client.SkylinkFromTUSURL(tc, uploader.Url())
	if err != nil {
		t.Fatal(err)
	}

	// Download the uploaded data and compare it to the uploaded data.
	downloadedData, err := r.SkynetSkylinkGet(skylink)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(uploadedData, downloadedData) {
		t.Fatal("data doesn't match")
	}
}
