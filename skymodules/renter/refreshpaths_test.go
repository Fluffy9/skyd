package renter

import (
	"fmt"
	"testing"
	"time"

	"gitlab.com/NebulousLabs/errors"
	"gitlab.com/NebulousLabs/fastrand"
	"gitlab.com/SkynetLabs/skyd/build"
	"gitlab.com/SkynetLabs/skyd/siatest/dependencies"
	"gitlab.com/SkynetLabs/skyd/skymodules"
	"go.sia.tech/siad/crypto"
	"go.sia.tech/siad/persist"
)

// TestRefreshPaths probes the refreshpaths subsystem
func TestRefreshPaths(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	t.Parallel()

	// Create a Renter
	rt, err := newRenterTesterWithDependency(t.Name(), &dependencies.DependencyDisableRepairAndHealthLoops{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := rt.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	// Create some directory tree paths
	paths := []skymodules.SiaPath{
		skymodules.RootSiaPath(),
		{Path: ""},
		{Path: "root"},
		{Path: "root/SubDir1"},
		{Path: "root/SubDir1/SubDir1"},
		{Path: "root/SubDir1/SubDir1/SubDir1"},
		{Path: "root/SubDir1/SubDir2"},
		{Path: "root/SubDir2"},
		{Path: "root/SubDir2/SubDir1"},
		{Path: "root/SubDir2/SubDir2"},
		{Path: "root/SubDir2/SubDir2/SubDir2"},
	}

	// Create a map of directories to be refreshed
	dirsToRefresh := rt.renter.callNewUniqueRefreshPaths()

	// Test adding a single child and making sure all the parents are added
	child := paths[len(paths)-1]
	parents := []skymodules.SiaPath{
		{Path: ""},
		{Path: "root"},
		{Path: "root/SubDir2"},
		{Path: "root/SubDir2/SubDir2"},
	}
	err = dirsToRefresh.callAdd(child)
	if err != nil {
		t.Fatal(err)
	}
	dirsToRefresh.mu.Lock()
	if _, ok := dirsToRefresh.childDirs[child]; !ok {
		t.Fatal("Did not find path in map", child)
	}
	for _, parent := range parents {
		if _, ok := dirsToRefresh.parentDirs[parent]; !ok {
			t.Fatal("Did not find path in map", parent)
		}
	}
	dirsToRefresh.mu.Unlock()

	// Reset
	dirsToRefresh = rt.renter.callNewUniqueRefreshPaths()

	// Add all paths to map
	for _, path := range paths {
		err = dirsToRefresh.callAdd(path)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Randomly add more paths
	for i := 0; i < 10; i++ {
		err = dirsToRefresh.callAdd(paths[fastrand.Intn(len(paths))])
		if err != nil {
			t.Fatal(err)
		}
	}

	// Verify the child and parent dir maps
	uniquePaths := []skymodules.SiaPath{
		{Path: "root/SubDir1/SubDir1/SubDir1"},
		{Path: "root/SubDir1/SubDir2"},
		{Path: "root/SubDir2/SubDir1"},
		{Path: "root/SubDir2/SubDir2/SubDir2"},
	}
	childDirs := dirsToRefresh.callNumChildDirs()
	if childDirs != len(uniquePaths) {
		t.Fatalf("Expected %v paths in child dir map but got %v", len(uniquePaths), childDirs)
	}
	dirsToRefresh.mu.Lock()
	for _, path := range uniquePaths {
		if _, ok := dirsToRefresh.childDirs[path]; !ok {
			t.Fatal("Did not find path in map", path)
		}
	}
	dirsToRefresh.mu.Unlock()
	parentPaths := []skymodules.SiaPath{
		{Path: ""},
		{Path: "root"},
		{Path: "root/SubDir1"},
		{Path: "root/SubDir1/SubDir1"},
		{Path: "root/SubDir2"},
		{Path: "root/SubDir2/SubDir2"},
	}
	parentDir := dirsToRefresh.callNumParentDirs()
	if parentDir != len(parentPaths) {
		t.Fatalf("Expected %v paths in parent dir map but got %v", len(parentPaths), parentDir)
	}
	dirsToRefresh.mu.Lock()
	for _, path := range parentPaths {
		if _, ok := dirsToRefresh.parentDirs[path]; !ok {
			t.Fatal("Did not find path in map", path)
		}
	}
	dirsToRefresh.mu.Unlock()

	// Make child directories and add a file to each
	rsc, _ := skymodules.NewRSCode(1, 1)
	up := skymodules.FileUploadParams{
		Source:      "",
		ErasureCode: rsc,
	}
	for _, sp := range uniquePaths {
		err = rt.renter.CreateDir(sp, skymodules.DefaultDirPerm)
		if err != nil {
			t.Fatal(err)
		}
		up.SiaPath, err = sp.Join("testFile")
		if err != nil {
			t.Fatal(err)
		}
		err = rt.renter.staticFileSystem.NewSiaFile(up.SiaPath, up.Source, up.ErasureCode, crypto.GenerateSiaKey(crypto.RandomCipherType()), 100, persist.DefaultDiskPermissionsTest, up.DisablePartialChunk)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Check the metadata of the root directory. Because we added files by
	// directly calling the staticFileSystem, a bubble should not have been
	// triggered and therefore the number of total files should be 0
	di, err := rt.renter.DirList(skymodules.RootSiaPath())
	if err != nil {
		t.Fatal(err)
	}
	if di[0].AggregateNumFiles != 0 {
		t.Fatal("Expected AggregateNumFiles to be 0 but got", di[0].AggregateNumFiles)
	}
	if di[0].AggregateNumSubDirs != 0 {
		t.Fatal("Expected AggregateNumSubDirs to be 0 but got", di[0].AggregateNumSubDirs)
	}

	// Add the default folders to the uniqueRefreshPaths to have their information
	// bubbled as well
	err1 := dirsToRefresh.callAdd(skymodules.HomeFolder)
	err2 := dirsToRefresh.callAdd(skymodules.UserFolder)
	err3 := dirsToRefresh.callAdd(skymodules.VarFolder)
	err4 := dirsToRefresh.callAdd(skymodules.SkynetFolder)
	err5 := dirsToRefresh.callAdd(skymodules.BackupFolder)
	err = errors.Compose(err1, err2, err3, err4, err5)
	if err != nil {
		t.Fatal(err)
	}

	// Have uniqueBubblePaths call bubble
	dirsToRefresh.callRefreshAllBlocking()

	// Wait for root directory to show proper number of files and subdirs.
	numSubDirs := len(dirsToRefresh.parentDirs) + len(dirsToRefresh.childDirs) - 1
	err = build.Retry(100, 100*time.Millisecond, func() error {
		di, err = rt.renter.DirList(skymodules.RootSiaPath())
		if err != nil {
			return err
		}
		if int(di[0].AggregateNumFiles) != len(uniquePaths) {
			return fmt.Errorf("Expected AggregateNumFiles to be %v but got %v", len(uniquePaths), di[0].AggregateNumFiles)
		}
		if int(di[0].AggregateNumSubDirs) != numSubDirs {
			return fmt.Errorf("Expected AggregateNumSubDirs to be %v but got %v", numSubDirs, di[0].AggregateNumSubDirs)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
