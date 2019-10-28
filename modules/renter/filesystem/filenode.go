package filesystem

import (
	"math"
	"os"
	"path/filepath"

	"gitlab.com/NebulousLabs/Sia/build"
	"gitlab.com/NebulousLabs/Sia/modules"
	"gitlab.com/NebulousLabs/Sia/modules/renter/siafile"
	"gitlab.com/NebulousLabs/errors"
)

type (
	// FileNode is a node which references a SiaFile.
	FileNode struct {
		node

		*siafile.SiaFile
	}
)

// close calls the common close method.
func (n *FileNode) close() {
	n.node._close()
}

// managedClose calls close while holding the node's lock.
func (n *FileNode) managedClose() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.close()
}

// Close calls close on the underlying node and also removes the fNode from its
// parent.
func (n *FileNode) Close() {
	// If a parent exists, we need to lock it while closing a child.
	n.mu.Lock()
	parent := n.parent
	n.mu.Unlock()
	if parent != nil {
		parent.mu.Lock()
	}
	n.mu.Lock()
	if n.parent != parent {
		build.Critical("parent changed")
	}

	// Call common close method.
	n.node._close()

	// Remove node from parent if the current thread was the last one.
	removeDir := len(n.threads) == 0
	if removeDir {
		parent.removeFile(n)
	}
	// Unlock child and parent.
	n.mu.Unlock()
	if parent != nil {
		child := parent
		parent := parent.parent
		child.mu.Unlock() // child is the parent we locked before

		// Iteratively try to remove parents as long as children got removed.
		for removeDir && parent != nil {
			parent.mu.Lock()
			child.mu.Lock()
			removeDir = len(child.threads)+len(child.directories)+len(child.files) == 0
			if removeDir {
				parent.removeDir(child)
			}
			child.mu.Unlock()
			child, parent = parent, parent.parent
			child.mu.Unlock() // parent became child
		}
	}
}

// Copy copies a file node and returns the copy.
func (n *FileNode) Copy() *FileNode {
	return n.managedCopy()
}

// managedCopy copies a file node and returns the copy.
func (n *FileNode) managedCopy() *FileNode {
	n.mu.Lock()
	defer n.mu.Unlock()
	newNode := *n
	newNode.threadUID = newThreadUID()
	newNode.threads[newNode.threadUID] = newThreadInfo()
	return &newNode
}

// Delete deletes the fNode's underlying file from disk.
func (n *FileNode) managedDelete() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.SiaFile.Delete()
}

// managedFileInfo returns the FileInfo of the file node.
func (n *FileNode) managedFileInfo(siaPath modules.SiaPath, offline map[string]bool, goodForRenew map[string]bool, contracts map[string]modules.RenterContract) (modules.FileInfo, error) {
	// Build the FileInfo
	var onDisk bool
	localPath := n.LocalPath()
	if localPath != "" {
		_, err := os.Stat(localPath)
		onDisk = err == nil
	}
	_, _, health, stuckHealth, numStuckChunks := n.Health(offline, goodForRenew)
	_, redundancy, err := n.Redundancy(offline, goodForRenew)
	if err != nil {
		return modules.FileInfo{}, errors.AddContext(err, "failed to get n redundancy")
	}
	uploadProgress, uploadedBytes, err := n.UploadProgressAndBytes()
	if err != nil {
		return modules.FileInfo{}, errors.AddContext(err, "failed to get upload progress and bytes")
	}
	maxHealth := math.Max(health, stuckHealth)
	fileInfo := modules.FileInfo{
		AccessTime:       n.AccessTime(),
		Available:        redundancy >= 1,
		ChangeTime:       n.ChangeTime(),
		CipherType:       n.MasterKey().Type().String(),
		CreateTime:       n.CreateTime(),
		Expiration:       n.Expiration(contracts),
		FileMode:         n.Mode(),
		Filesize:         n.Size(),
		Health:           health,
		LocalPath:        localPath,
		MaxHealth:        maxHealth,
		MaxHealthPercent: HealthPercentage(maxHealth),
		ModificationTime: n.ModTime(),
		NumStuckChunks:   numStuckChunks,
		OnDisk:           onDisk,
		Recoverable:      onDisk || redundancy >= 1,
		Redundancy:       redundancy,
		Renewing:         true,
		SiaPath:          siaPath,
		Stuck:            numStuckChunks > 0,
		StuckHealth:      stuckHealth,
		UploadedBytes:    uploadedBytes,
		UploadProgress:   uploadProgress,
	}
	return fileInfo, nil
}

// managedRename renames the fNode's underlying file.
func (n *FileNode) managedRename(newName string, oldParent, newParent *DirNode) error {
	// Lock the parents. If they are the same, only lock one.
	if oldParent.staticUID == newParent.staticUID {
		oldParent.mu.Lock()
		defer oldParent.mu.Unlock()
	} else {
		oldParent.mu.Lock()
		defer oldParent.mu.Unlock()
		newParent.mu.Lock()
		defer newParent.mu.Unlock()
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	// Check that newParent doesn't have a file with that name already.
	if _, exists := newParent.files[newName]; exists {
		return ErrExists
	}
	newPath := filepath.Join(newParent.absPath(), newName) + modules.SiaFileExtension
	// Rename the file.
	err := n.SiaFile.Rename(newPath)
	if err == siafile.ErrPathOverload {
		return ErrExists
	}
	if err != nil {
		return err
	}
	// Remove file from old parent and add it to new parent.
	// TODO: iteratively remove parents like in Close
	oldParent.removeFile(n)
	// Update parent and name.
	n.parent = newParent
	*n.name = newName
	*n.path = newPath
	// Add file to new parent.
	n.parent.files[*n.name] = n
	return err
}

// cachedFileInfo returns information on a siafile. As a performance
// optimization, the fileInfo takes the maps returned by
// renter.managedContractUtilityMaps for many files at once.
func (n *FileNode) staticCachedInfo(siaPath modules.SiaPath) modules.FileInfo {
	md := n.Metadata()

	// Build the FileInfo
	var onDisk bool
	localPath := md.LocalPath
	if localPath != "" {
		_, err := os.Stat(localPath)
		onDisk = err == nil
	}
	maxHealth := math.Max(md.CachedHealth, md.CachedStuckHealth)
	fileInfo := modules.FileInfo{
		AccessTime:       md.AccessTime,
		Available:        md.CachedUserRedundancy >= 1,
		ChangeTime:       md.ChangeTime,
		CipherType:       md.StaticMasterKeyType.String(),
		CreateTime:       md.CreateTime,
		Expiration:       md.CachedExpiration,
		FileMode:         md.Mode,
		Filesize:         uint64(md.FileSize),
		Health:           md.CachedHealth,
		LocalPath:        localPath,
		MaxHealth:        maxHealth,
		MaxHealthPercent: HealthPercentage(maxHealth),
		ModificationTime: md.ModTime,
		NumStuckChunks:   md.NumStuckChunks,
		OnDisk:           onDisk,
		Recoverable:      onDisk || md.CachedUserRedundancy >= 1,
		Redundancy:       md.CachedUserRedundancy,
		Renewing:         true,
		SiaPath:          siaPath,
		Stuck:            md.NumStuckChunks > 0,
		StuckHealth:      md.CachedStuckHealth,
		UploadedBytes:    md.CachedUploadedBytes,
		UploadProgress:   md.CachedUploadProgress,
	}
	return fileInfo
}
