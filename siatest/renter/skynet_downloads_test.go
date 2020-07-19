package renter

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"

	"gitlab.com/NebulousLabs/Sia/modules"
	"gitlab.com/NebulousLabs/Sia/node/api"
	"gitlab.com/NebulousLabs/Sia/siatest"
	"gitlab.com/NebulousLabs/errors"
	"gitlab.com/NebulousLabs/fastrand"
)

// TestSkynetDownloads verifies the functionality of Skynet downloads.
func TestSkynetDownloads(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	t.Parallel()

	// Create a testgroup.
	groupParams := siatest.GroupParams{
		Hosts:   3,
		Miners:  1,
		Renters: 1,
	}
	groupDir := renterTestDir(t.Name())

	// Specify subtests to run
	subTests := []siatest.SubTest{
		{Name: "SingleFileRegular", Test: testDownloadSingleFileRegular},
		{Name: "SingleFileMultiPart", Test: testDownloadSingleFileMultiPart},
		{Name: "DirectoryBasic", Test: testDownloadDirectoryBasic},
		{Name: "DirectoryNested", Test: testDownloadDirectoryNested},
	}

	// Run tests
	if err := siatest.RunSubTests(t, groupParams, groupDir, subTests); err != nil {
		t.Fatal(err)
	}
}

// testDownloadSingleFileRegular tests the download of a single skyfile,
// uploaded using a regular stream.
func testDownloadSingleFileRegular(t *testing.T, tg *siatest.TestGroup) {
	r := tg.Renters()[0]

	// upload a single file using a stream
	size := fastrand.Uint64n(100) + 100
	data := fastrand.Bytes(int(size))
	skylink, sup, _, err := r.UploadNewSkyfileWithDataBlocking("SingleFileRegular", data, false)
	if err != nil {
		t.Fatal(err)
	}

	// verify downloads
	err = verifyDownloadRaw(t, r, skylink, data, sup.FileMetadata)
	if err != nil {
		t.Fatal(err)
	}
	err = verifyDownloadDirectory(t, r, skylink, data, sup.FileMetadata)
	if err != nil {
		t.Fatal(err)
	}
	err = verifyDownloadAsArchive(t, r, skylink, fileMap{"SingleFileRegular": data}, sup.FileMetadata)
	if err != nil {
		t.Fatal(err)
	}
}

// testDownloadSingleFileMultiPart tests the download of a single skyfile,
// uploaded using a multipart upload.
func testDownloadSingleFileMultiPart(t *testing.T, tg *siatest.TestGroup) {
	r := tg.Renters()[0]

	// upload a single file using multi-part upload
	data := []byte("contents_file1.png")
	files := []siatest.TestFile{{Name: "file1.png", Data: data}}
	skylink, _, _, err := r.UploadNewMultipartSkyfileBlocking("SingleFileMultiPart", files, "", false, false)
	if err != nil {
		t.Fatal(err)
	}

	// construct the metadata object we expect to be returned
	expectedMetadata := modules.SkyfileMetadata{
		Filename: "SingleFileMultiPart",
		Subfiles: map[string]modules.SkyfileSubfileMetadata{
			"file1.png": {
				FileMode:    os.FileMode(0644),
				Filename:    "file1.png",
				ContentType: "application/octet-stream",
				Offset:      0,
				Len:         uint64(len(data)),
			}},
		DefaultPath: "/file1.png",
	}
	// verify downloads
	err = verifyDownloadRaw(t, r, skylink, data, expectedMetadata)
	if err != nil {
		t.Fatal(err)
	}
	err = verifyDownloadAsArchive(t, r, skylink, fileMapFromFiles(files), expectedMetadata)
	if err != nil {
		t.Fatal(err)
	}

	// verify non existing default path
	_, _, _, err = r.UploadNewMultipartSkyfileBlocking("multipartUploadSingle", files, "notexists.png", false, false)
	if err == nil || !strings.Contains(err.Error(), api.ErrInvalidDefaultPath.Error()) {
		t.Errorf("Expected '%v' instead error was '%v'", api.ErrInvalidDefaultPath, err)
	}

	// verify trying to set no default path on single file upload
	_, _, _, err = r.UploadNewMultipartSkyfileBlocking("multipartUploadSingle", files, "", false, false)
	if err != nil {
		t.Errorf("Expected success, instead error was '%v'", err)
	}
}

// testDownloadDirectoryBasic tests the download of a directory skyfile
func testDownloadDirectoryBasic(t *testing.T, tg *siatest.TestGroup) {
	r := tg.Renters()[0]

	// upload a multi-file skyfile
	files := []siatest.TestFile{
		{Name: "index.html", Data: []byte("index.html_contents")},
		{Name: "about.html", Data: []byte("about.html_contents")},
	}
	skylink, _, _, err := r.UploadNewMultipartSkyfileBlocking("DirectoryBasic", files, "", false, false)
	if err != nil {
		t.Fatal(err)
	}

	// construct the metadata object we expect to be returned
	expectedMetadata := modules.SkyfileMetadata{
		Filename: "DirectoryBasic",
		Subfiles: map[string]modules.SkyfileSubfileMetadata{
			"index.html": {
				FileMode:    os.FileMode(0644),
				Filename:    "index.html",
				ContentType: "application/octet-stream",
				Offset:      0,
				Len:         uint64(len(files[0].Data)),
			},
			"about.html": {
				FileMode:    os.FileMode(0644),
				Filename:    "about.html",
				ContentType: "application/octet-stream",
				Offset:      uint64(len(files[0].Data)),
				Len:         uint64(len(files[1].Data)),
			}},
		DefaultPath:        "/index.html",
		DisableDefaultPath: false,
	}

	// verify downloads
	err = verifyDownloadRaw(t, r, skylink, files[0].Data, expectedMetadata)
	if err != nil {
		t.Fatal(err)
	}
	err = verifyDownloadDirectory(t, r, skylink, append(files[0].Data, files[1].Data...), expectedMetadata)
	if err != nil {
		t.Fatal(err)
	}
	err = verifyDownloadAsArchive(t, r, skylink, fileMapFromFiles(files), expectedMetadata)
	if err != nil {
		t.Fatal(err)
	}

	// upload the same files but with a different default path
	skylink, _, _, err = r.UploadNewMultipartSkyfileBlocking("DirectoryBasic", files, "about.html", false, true)
	if err != nil {
		t.Fatal(err)
	}

	// construct the metadata object we expect to be returned
	expectedMetadata = modules.SkyfileMetadata{
		Filename: "DirectoryBasic",
		Subfiles: map[string]modules.SkyfileSubfileMetadata{
			"index.html": {
				FileMode:    os.FileMode(0644),
				Filename:    "index.html",
				ContentType: "application/octet-stream",
				Offset:      0,
				Len:         uint64(len(files[0].Data)),
			},
			"about.html": {
				FileMode:    os.FileMode(0644),
				Filename:    "about.html",
				ContentType: "application/octet-stream",
				Offset:      uint64(len(files[0].Data)),
				Len:         uint64(len(files[1].Data)),
			}},
		DefaultPath: "/about.html",
	}

	// verify downloads
	err = verifyDownloadRaw(t, r, skylink, files[1].Data, expectedMetadata)
	if err != nil {
		t.Fatal(err)
	}
	err = verifyDownloadAsArchive(t, r, skylink, fileMapFromFiles(files), expectedMetadata)
	if err != nil {
		t.Fatal(err)
	}

	// upload the same files but with no default path
	skylink, _, _, err = r.UploadNewMultipartSkyfileBlocking("DirectoryBasic", files, "", true, true)
	if err != nil {
		t.Fatal(err)
	}

	// construct the metadata object we expect to be returned
	expectedMetadata = modules.SkyfileMetadata{
		Filename: "DirectoryBasic",
		Subfiles: map[string]modules.SkyfileSubfileMetadata{
			"index.html": {
				FileMode:    os.FileMode(0644),
				Filename:    "index.html",
				ContentType: "application/octet-stream",
				Offset:      0,
				Len:         uint64(len(files[0].Data)),
			},
			"about.html": {
				FileMode:    os.FileMode(0644),
				Filename:    "about.html",
				ContentType: "application/octet-stream",
				Offset:      uint64(len(files[0].Data)),
				Len:         uint64(len(files[1].Data)),
			},
		},
		DefaultPath:        "",
		DisableDefaultPath: true,
	}

	// verify downloads
	err = verifyDownloadDirectory(t, r, skylink, append(files[0].Data, files[1].Data...), expectedMetadata)
	if err != nil {
		t.Fatal(err)
	}

	// verify some errors on upload
	skylink, _, _, err = r.UploadNewMultipartSkyfileBlocking("DirectoryBasic", files, "notexists.html", false, false)
	if err == nil || !strings.Contains(err.Error(), api.ErrInvalidDefaultPath.Error()) {
		t.Errorf("Expected '%v' instead error was '%v'", api.ErrInvalidDefaultPath, err)
	}
}

// testDownloadDirectoryNested tests the download of a directory skyfile with
// a nested directory structure
func testDownloadDirectoryNested(t *testing.T, tg *siatest.TestGroup) {
	r := tg.Renters()[0]

	// upload a multi-file skyfile with a nested file structure
	files := []siatest.TestFile{
		{Name: "assets/images/file1.png", Data: []byte("file1.png_contents")},
		{Name: "assets/images/file2.png", Data: []byte("file2.png_contents")},
		{Name: "assets/index.html", Data: []byte("assets_index.html_contents")},
		{Name: "index.html", Data: []byte("index.html_contents")},
	}
	skylink, _, _, err := r.UploadNewMultipartSkyfileBlocking("DirectoryNested", files, "", false, false)
	if err != nil {
		t.Fatal(err)
	}

	// note that index.html is listed first but is uploaded as the last file
	expectedMetadata := modules.SkyfileMetadata{
		Filename: "DirectoryNested",
		Subfiles: map[string]modules.SkyfileSubfileMetadata{
			"index.html": {
				FileMode:    os.FileMode(0644),
				Filename:    "index.html",
				ContentType: "application/octet-stream",
				Offset:      uint64(len(files[0].Data) + len(files[1].Data) + len(files[2].Data)),
				Len:         uint64(len(files[3].Data)),
			},
			"assets/images/file1.png": {
				FileMode:    os.FileMode(0644),
				Filename:    "assets/images/file1.png",
				ContentType: "application/octet-stream",
				Offset:      0,
				Len:         uint64(len(files[0].Data)),
			},
			"assets/images/file2.png": {
				FileMode:    os.FileMode(0644),
				Filename:    "assets/images/file2.png",
				ContentType: "application/octet-stream",
				Offset:      uint64(len(files[0].Data)),
				Len:         uint64(len(files[1].Data)),
			},
			"assets/index.html": {
				FileMode:    os.FileMode(0644),
				Filename:    "assets/index.html",
				ContentType: "application/octet-stream",
				Offset:      uint64(len(files[0].Data) + len(files[1].Data)),
				Len:         uint64(len(files[2].Data)),
			},
		},
		DefaultPath:        "/index.html",
		DisableDefaultPath: false,
	}

	// verify downloads
	err = verifyDownloadRaw(t, r, skylink, files[3].Data, expectedMetadata)
	if err != nil {
		t.Fatal(err)
	}
	err = verifyDownloadAsArchive(t, r, skylink, fileMapFromFiles(files), expectedMetadata)
	if err != nil {
		t.Fatal(err)
	}

	// verify downloading a subdirectory
	expectedMetadata = modules.SkyfileMetadata{
		Filename: "/assets/images",
		Subfiles: map[string]modules.SkyfileSubfileMetadata{
			"assets/images/file1.png": {
				FileMode:    os.FileMode(0644),
				Filename:    "assets/images/file1.png",
				ContentType: "application/octet-stream",
				Offset:      0,
				Len:         uint64(len(files[0].Data)),
			},
			"assets/images/file2.png": {
				FileMode:    os.FileMode(0644),
				Filename:    "assets/images/file2.png",
				ContentType: "application/octet-stream",
				Offset:      uint64(len(files[0].Data)),
				Len:         uint64(len(files[1].Data)),
			},
		},
	}

	err = verifyDownloadDirectory(t, r, skylink+"/assets/images", append(files[0].Data, files[1].Data...), expectedMetadata)
	if err != nil {
		t.Fatal(err)
	}
	err = verifyDownloadAsArchive(t, r, skylink+"/assets/images",
		fileMapFromFiles(files[:2]), expectedMetadata)
	if err != nil {
		t.Fatal(err)
	}

	// verify downloading a nested file
	err = verifyDownloadRaw(t, r, skylink+"/assets/index.html", files[2].Data, modules.SkyfileMetadata{
		Filename: "/assets/index.html",
		Subfiles: map[string]modules.SkyfileSubfileMetadata{
			"assets/index.html": {
				FileMode:    os.FileMode(0644),
				Filename:    "assets/index.html",
				ContentType: "application/octet-stream",
				Offset:      0,
				Len:         uint64(len(files[2].Data)),
			},
		},
	},
	)
	if err != nil {
		t.Fatal(err)
	}

	// upload the same files with the nested index.html as default
	files = []siatest.TestFile{
		{Name: "assets/images/file1.png", Data: []byte("file1.png_contents")},
		{Name: "assets/images/file2.png", Data: []byte("file2.png_contents")},
		{Name: "assets/index.html", Data: []byte("assets_index.html_contents")},
		{Name: "index.html", Data: []byte("index.html_contents")},
	}
	skylink, _, _, err = r.UploadNewMultipartSkyfileBlocking("DirectoryNested", files, "assets/index.html", false, true)
	if err != nil {
		t.Fatal(err)
	}

	err = verifyDownloadRaw(t, r, skylink, files[2].Data, modules.SkyfileMetadata{
		Filename: "DirectoryNested",
		Subfiles: map[string]modules.SkyfileSubfileMetadata{
			"index.html": {
				FileMode:    os.FileMode(0644),
				Filename:    "index.html",
				ContentType: "application/octet-stream",
				Offset:      uint64(len(files[0].Data) + len(files[1].Data) + len(files[2].Data)),
				Len:         uint64(len(files[3].Data)),
			},
			"assets/images/file1.png": {
				FileMode:    os.FileMode(0644),
				Filename:    "assets/images/file1.png",
				ContentType: "application/octet-stream",
				Offset:      0,
				Len:         uint64(len(files[0].Data)),
			},
			"assets/images/file2.png": {
				FileMode:    os.FileMode(0644),
				Filename:    "assets/images/file2.png",
				ContentType: "application/octet-stream",
				Offset:      uint64(len(files[0].Data)),
				Len:         uint64(len(files[1].Data)),
			},
			"assets/index.html": {
				FileMode:    os.FileMode(0644),
				Filename:    "assets/index.html",
				ContentType: "application/octet-stream",
				Offset:      uint64(len(files[0].Data) + len(files[1].Data)),
				Len:         uint64(len(files[2].Data)),
			},
		},
		DefaultPath: "/assets/index.html",
	})
	if err != nil {
		t.Fatal(err)
	}
}

// fileMapFromFiles is a helper that converts a list of test files to a file map
func fileMapFromFiles(tfs []siatest.TestFile) fileMap {
	fm := make(fileMap)
	for _, tf := range tfs {
		fm[tf.Name] = tf.Data
	}
	return fm
}

// verifyDownloadRaw is a helper function that downloads the content for the
// given skylink and verifies the response data and response headers.
func verifyDownloadRaw(t *testing.T, r *siatest.TestNode, skylink string, expectedData []byte, expectedMetadata modules.SkyfileMetadata) error {
	data, metadata, err := r.SkynetSkylinkGet(skylink)
	if err != nil {
		return err
	}
	if !bytes.Equal(data, expectedData) {
		t.Log("expected data: ", string(expectedData))
		t.Log("actual   data: ", string(data))
		return errors.New("Unexpected data")
	}
	if !reflect.DeepEqual(metadata, expectedMetadata) {
		t.Log("expected metadata: ", expectedMetadata)
		t.Log("actual   metadata: ", metadata)
		return errors.New("Unexpected metadata")
	}
	return nil
}

// verifyDownloadRaw is a helper function that downloads the content for the
// given skylink and verifies the response data and response headers. It will
// download the file using the `concat` format to be able to compare the data
// without it having to be an archive.
func verifyDownloadDirectory(t *testing.T, r *siatest.TestNode, skylink string, expectedData []byte, expectedMetadata modules.SkyfileMetadata) error {
	data, metadata, err := r.SkynetSkylinkConcatGet(skylink)
	if err != nil {
		return err
	}
	if !bytes.Equal(data, expectedData) {
		t.Log("expected data: ", expectedData)
		t.Log("actual   data: ", data)
		return errors.New("Unexpected data")
	}
	if !reflect.DeepEqual(metadata, expectedMetadata) {
		t.Log("expected metadata: ", expectedMetadata)
		t.Log("actual   metadata: ", metadata)
		return errors.New("Unexpected metadata")
	}
	return nil
}

// verifyDownloadAsArchive is a helper function that downloads the content for
// the given skylink and verifies the response data and response headers. It
// will download the file using all of the archive formats we support, verifying
// the contents of the archive for every type.
func verifyDownloadAsArchive(t *testing.T, r *siatest.TestNode, skylink string, expectedFiles fileMap, expectedMetadata modules.SkyfileMetadata) error {
	// zip
	header, reader, err := r.SkynetSkylinkZipReaderGet(skylink)
	if err != nil {
		return err
	}

	files, err := readZipArchive(reader)
	if err != nil {
		return err
	}
	err = reader.Close()
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(files, expectedFiles) {
		t.Log("expected:", expectedFiles)
		t.Log("actual  :", files)
		return errors.New("Unexpected files")
	}
	ct := header.Get("Content-type")
	if ct != "application/zip" {
		return fmt.Errorf("Unexpected 'Content-Type' header, expected 'application/zip' actual '%v'", ct)
	}

	var md modules.SkyfileMetadata
	mdStr := header.Get("Skynet-File-Metadata")
	if mdStr != "" {
		err = json.Unmarshal([]byte(mdStr), &md)
		if err != nil {
			return errors.AddContext(err, "could not unmarshal metadata")
		}
	}

	if !reflect.DeepEqual(md, expectedMetadata) {
		t.Log("expected:", expectedMetadata)
		t.Log("actual  :", md)
		return errors.New("Unexpected metadata")
	}

	// tar
	header, reader, err = r.SkynetSkylinkTarReaderGet(skylink)
	if err != nil {
		return err
	}
	files, err = readTarArchive(reader)
	if err != nil {
		return err
	}
	err = reader.Close()
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(files, expectedFiles) {
		t.Log("expected:", expectedFiles)
		t.Log("actual  :", files)
		return errors.New("Unexpected files")
	}
	ct = header.Get("Content-type")
	if ct != "application/x-tar" {
		return fmt.Errorf("Unexpected 'Content-Type' header, expected 'application/x-tar' actual '%v'", ct)
	}

	mdStr = header.Get("Skynet-File-Metadata")
	if mdStr != "" {
		err = json.Unmarshal([]byte(mdStr), &md)
		if err != nil {
			return errors.AddContext(err, "could not unmarshal metadata")
		}
	}

	if !reflect.DeepEqual(md, expectedMetadata) {
		t.Log("expected:", expectedMetadata)
		t.Log("actual  :", md)
		return errors.New("Unexpected metadata")
	}

	// tar gz
	header, reader, err = r.SkynetSkylinkTarGzReaderGet(skylink)
	if err != nil {
		return err
	}
	gzr, err := gzip.NewReader(reader)
	if err != nil {
		return err
	}
	files, err = readTarArchive(gzr)
	if err != nil {
		return err
	}
	err = errors.Compose(reader.Close(), gzr.Close())
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(files, expectedFiles) {
		t.Log("expected:", expectedFiles)
		t.Log("actual  :", files)
		return errors.New("Unexpected files")
	}
	ct = header.Get("Content-type")
	if ct != "application/x-gtar" {
		return fmt.Errorf("Unexpected 'Content-Type' header, expected 'application/x-gtar' actual '%v'", ct)
	}

	mdStr = header.Get("Skynet-File-Metadata")
	if mdStr != "" {
		err = json.Unmarshal([]byte(mdStr), &md)
		if err != nil {
			return errors.AddContext(err, "could not unmarshal metadata")
		}
	}
	if !reflect.DeepEqual(md, expectedMetadata) {
		t.Log("expected:", expectedMetadata)
		t.Log("actual  :", md)
		return errors.New("Unexpected metadata")
	}

	return nil
}
