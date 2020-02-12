package contractmanager

import (
	"errors"
	"sync"
	"sync/atomic"

	"gitlab.com/NebulousLabs/Sia/build"
	"gitlab.com/NebulousLabs/Sia/modules"
)

type (
	// storageFolderExtension is the data saved to the WAL to indicate that a
	// storage folder has been extended successfully.
	storageFolderExtension struct {
		Index          uint16
		NewSectorCount uint32
	}

	// unfinishedStorageFolderExtension contains the data necessary to reverse
	// a storage folder extension that has failed.
	unfinishedStorageFolderExtension struct {
		Index          uint16
		OldSectorCount uint32
	}
)

// findUnfinishedStorageFolderExtensions will scroll through a set of state
// changes as pull out all of the storage folder extensions which have not yet
// completed.
//func findUnfinishedStorageFolderExtensions(scs []stateChange) []unfinishedStorageFolderExtension {
//	// Use a map to figure out what unfinished storage folder extensions exist
//	// and use it to remove the ones that have terminated.
//	usfeMap := make(map[uint16]unfinishedStorageFolderExtension)
//	for _, sc := range scs {
//		for _, usfe := range sc.UnfinishedStorageFolderExtensions {
//			usfeMap[usfe.Index] = usfe
//		}
//		for _, sfe := range sc.StorageFolderExtensions {
//			delete(usfeMap, sfe.Index)
//		}
//		for _, index := range sc.ErroredStorageFolderExtensions {
//			delete(usfeMap, index)
//		}
//		for _, sfr := range sc.StorageFolderRemovals {
//			delete(usfeMap, sfr.Index)
//		}
//	}
//
//	// Return the active unifinished storage folder extensions as a slice.
//	usfes := make([]unfinishedStorageFolderExtension, 0, len(usfeMap))
//	for _, usfe := range usfeMap {
//		usfes = append(usfes, usfe)
//	}
//	return usfes
//}

// cleanupUnfinishedStorageFolderExtensions will reset any unsuccessful storage
// folder extensions from the previous run.
//func (cm *ContractManager) cleanupUnfinishedStorageFolderExtensions(scs []stateChange) {
//	usfes := findUnfinishedStorageFolderExtensions(scs)
//	for _, usfe := range usfes {
//		sf, exists := cm.storageFolders[usfe.Index]
//		if !exists || atomic.LoadUint64(&sf.atomicUnavailable) == 1 {
//			cm.log.Critical("unfinished storage folder extension exists where the storage folder does not exist")
//			continue
//		}
//
//		// Truncate the files back to their original size.
//		err := sf.metadataFile.Truncate(int64(len(sf.usage) * storageFolderGranularity * sectorMetadataDiskSize))
//		if err != nil {
//			cm.log.Printf("Error: unable to truncate metadata file as storage folder %v is resized\n", sf.path)
//		}
//		err = sf.sectorFile.Truncate(int64(modules.SectorSize * storageFolderGranularity * uint64(len(sf.usage))))
//		if err != nil {
//			cm.log.Printf("Error: unable to truncate sector file as storage folder %v is resized\n", sf.path)
//		}
//
//		// Append an error call to the changeset, indicating that the storage
//		// folder add was not completed successfully.
//		wal.appendChange(stateChange{
//			ErroredStorageFolderExtensions: []uint16{sf.index},
//		})
//	}
//}

// commitStorageFolderExtension will apply a storage folder extension to the
// state.
func (cm *ContractManager) commitStorageFolderExtension(sfe storageFolderExtension) {
	sf, exists := cm.storageFolders[sfe.Index]
	if !exists || atomic.LoadUint64(&sf.atomicUnavailable) == 1 {
		cm.log.Critical("ERROR: storage folder extension provided for storage folder that does not exist")
		return
	}

	newUsageSize := sfe.NewSectorCount / storageFolderGranularity
	appendUsage := make([]uint64, int(newUsageSize)-len(sf.usage))
	sf.usage = append(sf.usage, appendUsage...)
}

// growStorageFolder will extend the storage folder files so that they may hold
// more sectors.
func (cm *ContractManager) growStorageFolder(index uint16, newSectorCount uint32) error {
	// Retrieve the specified storage folder.
	cm.mu.Lock()
	sf, exists := cm.storageFolders[index]
	cm.mu.Unlock()
	if !exists || atomic.LoadUint64(&sf.atomicUnavailable) == 1 {
		return errStorageFolderNotFound
	}

	// Lock the storage folder for the duration of the operation.
	sf.mu.Lock()
	defer sf.mu.Unlock()

	// Write the intention to increase the storage folder size to the WAL,
	// providing enough information to allow a truncation if the growing fails.
	oldSectorCount := uint32(len(sf.usage)) * storageFolderGranularity

	// Prepare variables for growing the storage folder.
	currentHousingSize := int64(len(sf.usage)) * int64(modules.SectorSize) * storageFolderGranularity
	currentMetadataSize := int64(len(sf.usage)) * sectorMetadataDiskSize * storageFolderGranularity
	newHousingSize := int64(newSectorCount) * int64(modules.SectorSize)
	newMetadataSize := int64(newSectorCount) * sectorMetadataDiskSize
	if newHousingSize <= currentHousingSize || newMetadataSize <= currentMetadataSize {
		cm.log.Critical("growStorageFolder called without size increase", newHousingSize, currentHousingSize, newMetadataSize, currentMetadataSize)
		return errors.New("unable to make the requested change, please notify the devs that there is a bug")
	}
	housingWriteSize := newHousingSize - currentHousingSize
	metadataWriteSize := newMetadataSize - currentMetadataSize

	// If there's an error in the rest of the function, reset the storage
	// folders to their original size.
	var err error
	defer func(sf *storageFolder, housingSize, metadataSize int64) {
		if err != nil {
			cm.mu.Lock()
			defer cm.mu.Unlock()

			// Remove the leftover files from the failed operation.
			err = build.ComposeErrors(err, sf.metadataFile.Truncate(housingSize))
			err = build.ComposeErrors(err, sf.sectorFile.Truncate(metadataSize))

			// Signal in the WAL that the unfinished storage folder addition
			// has failed.
			err = build.ComposeErrors(err, cm.shrinkStorageFolder(index, oldSectorCount, true))
		}
	}(sf, currentMetadataSize, currentHousingSize)

	// Extend the sector file and metadata file on disk.
	atomic.StoreUint64(&sf.atomicProgressDenominator, uint64(housingWriteSize+metadataWriteSize))

	var updates []walUpdate
	stepCount := housingWriteSize / folderAllocationStepSize
	for i := int64(0); i < stepCount; i++ {
		updates = append(updates, truncateUpdate(sf.sectorFile, currentHousingSize+(folderAllocationStepSize*(i+1))))
		// After each iteration, update the progress numerator.
		// TODO: this is no longer accurate
		atomic.AddUint64(&sf.atomicProgressNumerator, folderAllocationStepSize)
	}
	updates = append(updates, truncateUpdate(sf.sectorFile, currentHousingSize+housingWriteSize))

	// Write the metadata file.
	updates = append(updates, truncateUpdate(sf.metadataFile, currentMetadataSize+metadataWriteSize))

	// Apply the changes.
	if err := cm.createAndApplyTransaction(updates...); err != nil {
		return err
	}

	// The file creation process is essentially complete at this point, report
	// complete progress.
	atomic.StoreUint64(&sf.atomicProgressNumerator, uint64(housingWriteSize+metadataWriteSize))

	// Sync the files.
	var err1, err2 error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		err1 = sf.metadataFile.Sync()
		if err != nil {
			cm.log.Println("could not synchronize allocated sector metadata file:", err)
		}
	}()
	go func() {
		defer wg.Done()
		err2 = sf.sectorFile.Sync()
		if err != nil {
			cm.log.Println("could not synchronize allocated sector data file:", err)
		}
	}()
	wg.Wait()
	if err1 != nil || err2 != nil {
		err = build.ComposeErrors(err1, err2)
		cm.log.Println("cound not synchronize storage folder extensions:", err)
		return build.ExtendErr("unable to synchronize storage folder extensions", err)
	}

	// Simulate power failure at this point for some testing scenarios.
	if cm.dependencies.Disrupt("incompleteGrowStorageFolder") {
		return nil
	}

	// Storage folder growth has completed successfully.
	cm.mu.Lock()
	cm.storageFolders[sf.index] = sf
	cm.mu.Unlock()

	// Set the progress back to '0'.
	atomic.StoreUint64(&sf.atomicProgressNumerator, 0)
	atomic.StoreUint64(&sf.atomicProgressDenominator, 0)
	return nil
}
