package renter

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/cipher"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"gitlab.com/NebulousLabs/errors"
	"gitlab.com/NebulousLabs/fastrand"
	"golang.org/x/crypto/twofish"

	"gitlab.com/NebulousLabs/Sia/build"
	"gitlab.com/NebulousLabs/Sia/crypto"
	"gitlab.com/NebulousLabs/Sia/modules"
	"gitlab.com/NebulousLabs/Sia/modules/renter/siadir"
	"gitlab.com/NebulousLabs/Sia/modules/renter/siafile"
)

// backupHeader defines the structure of the backup's JSON header.
type backupHeader struct {
	Version    string `json:"version"`
	Encryption string `json:"encryption"`
	IV         []byte `json:"iv"`
}

// The following specifiers are options for the encryption of backups.
var (
	encryptionPlaintext = "plaintext"
	encryptionTwofish   = "twofish-ctr"
	encryptionVersion   = "1.0"
)

// CreateBackup creates a backup of the renter's siafiles. If a secret is not
// nil, the backup will be encrypted using the provided secret.
func (r *Renter) CreateBackup(dst string, secret []byte) error {
	if err := r.tg.Add(); err != nil {
		return err
	}
	defer r.tg.Done()
	return r.managedCreateBackup(dst, secret)
}

// managedCreateBackup creates a backup of the renter's siafiles. If a secret is
// not nil, the backup will be encrypted using the provided secret.
func (r *Renter) managedCreateBackup(dst string, secret []byte) error {
	// Create the gzip file.
	f, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer f.Close()
	archive := io.Writer(f)

	// Prepare a header for the backup and default to no encryption. This will
	// potentially be overwritten later.
	bh := backupHeader{
		Version:    encryptionVersion,
		Encryption: encryptionPlaintext,
	}

	// Wrap it for encryption if required.
	if secret != nil {
		bh.Encryption = encryptionTwofish
		bh.IV = fastrand.Bytes(twofish.BlockSize)
		c, err := twofish.NewCipher(secret)
		if err != nil {
			return err
		}
		sw := cipher.StreamWriter{
			S: cipher.NewCTR(c, bh.IV),
			W: archive,
		}
		archive = sw
	}

	// Skip the checkum for now.
	if _, err := f.Seek(crypto.HashSize, io.SeekStart); err != nil {
		return err
	}
	// Write the header.
	enc := json.NewEncoder(f)
	if err := enc.Encode(bh); err != nil {
		return err
	}
	// Wrap the archive in a multiwriter to hash the contents of the archive
	// before encrypting it.
	h := crypto.NewHash()
	archive = io.MultiWriter(archive, h)
	// Wrap the potentially encrypted writer into a gzip writer.
	gzw := gzip.NewWriter(archive)
	// Wrap the gzip writer into a tar writer.
	tw := tar.NewWriter(gzw)
	// Tar the partials Siafiles first.
	if err := r.managedTarPartialsSiaFile(tw); err != nil {
		twErr := tw.Close()
		gzwErr := gzw.Close()
		return errors.AddContext(errors.Compose(err, twErr, gzwErr), "failed to tar partials sia file")
	}
	// Add the remaining files to the archive.
	if err := r.managedTarSiaFiles(tw); err != nil {
		twErr := tw.Close()
		gzwErr := gzw.Close()
		return errors.Compose(err, twErr, gzwErr)
	}
	// Close writers to flush them before computing the hash.
	twErr := tw.Close()
	gzwErr := gzw.Close()
	// Write the hash to the beginning of the file.
	_, err = f.WriteAt(h.Sum(nil), 0)
	return errors.Compose(err, twErr, gzwErr)
}

// LoadBackup loads the siafiles of a previously created backup into the
// renter. If the backup is encrypted, secret will be used to decrypt it.
// Otherwise the argument is ignored.
func (r *Renter) LoadBackup(src string, secret []byte) error {
	if err := r.tg.Add(); err != nil {
		return err
	}
	defer r.tg.Done()

	// Only load a backup if there are no siafiles yet.
	root, err := r.staticDirSet.Open(modules.RootSiaPath())
	if err != nil {
		return err
	}
	defer root.Close()

	// Open the gzip file.
	f, err := os.Open(src)
	if err != nil {
		return err
	}
	defer f.Close()
	archive := io.Reader(f)

	// Read the checksum.
	var chks crypto.Hash
	_, err = io.ReadFull(f, chks[:])
	if err != nil {
		return err
	}
	// Read the header.
	dec := json.NewDecoder(archive)
	var bh backupHeader
	if err := dec.Decode(&bh); err != nil {
		return err
	}
	// Seek back by the amount of data left in the decoder's buffer. That gives
	// us the offset of the body.
	var off int64
	if buf, ok := dec.Buffered().(*bytes.Reader); ok {
		off, err = f.Seek(int64(1-buf.Len()), io.SeekCurrent)
		if err != nil {
			return err
		}
	} else {
		build.Critical("Buffered should return a bytes.Reader")
	}
	// Check the version number.
	if bh.Version != encryptionVersion {
		return errors.New("unknown version")
	}
	// Wrap the file in the correct streamcipher.
	archive, err = wrapReaderInCipher(f, bh, secret)
	if err != nil {
		return err
	}
	// Pipe the remaining file into the hasher to verify that the hash is
	// correct.
	h := crypto.NewHash()
	_, err = io.Copy(h, archive)
	if err != nil {
		return err
	}
	// Verify the hash.
	if !bytes.Equal(h.Sum(nil), chks[:]) {
		return errors.New("checksum doesn't match")
	}
	// Seek back to the beginning of the body.
	if _, err := f.Seek(off, io.SeekStart); err != nil {
		return err
	}
	// Wrap the file again.
	archive, err = wrapReaderInCipher(f, bh, secret)
	if err != nil {
		return err
	}
	// Wrap the potentially encrypted reader in a gzip reader.
	gzr, err := gzip.NewReader(archive)
	if err != nil {
		return err
	}
	defer gzr.Close()
	// Wrap the gzip reader in a tar reader.
	tr := tar.NewReader(gzr)
	// Untar the files.
	return r.managedUntarDir(tr)
}

// managedTarPartialsSiaFile tars only partials Siafiles. This makes sure than
// when untaring the archive, we read those files first.
func (r *Renter) managedTarPartialsSiaFile(tw *tar.Writer) error {
	// Walk over all the partials siafiles and add them to the tarball.
	return filepath.Walk(r.staticFilesDir, func(path string, info os.FileInfo, err error) error {
		// This error is non-nil if filepath.Walk couldn't stat a file or
		// folder.
		if err != nil {
			return err
		}
		// Nothing to do for files that are not PartialsSiaFiles
		if filepath.Ext(path) != modules.PartialsSiaFileExtension {
			return nil
		}
		// Create the header for the file/dir.
		header, err := tar.FileInfoHeader(info, info.Name())
		if err != nil {
			return err
		}
		relPath := strings.TrimPrefix(path, r.staticFilesDir)
		header.Name = relPath
		// Get the siafile.
		siaPathStr := strings.TrimSuffix(relPath, modules.SiaFileExtension)
		siaPathStr = strings.TrimSuffix(siaPathStr, modules.PartialsSiaFileExtension)
		siaPath, err := modules.NewSiaPath(siaPathStr)
		if err != nil {
			return err
		}
		entry, err := r.staticFileSet.LoadPartialSiaFile(siaPath)
		if err != nil {
			return err
		}
		defer entry.Close()
		// Get a reader to read from the siafile.
		sr, err := entry.SnapshotReader()
		if err != nil {
			return err
		}
		defer sr.Close()
		// Update the size of the file within the header since it might have changed
		// while we weren't holding the lock.
		fi, err := sr.Stat()
		if err != nil {
			return err
		}
		header.Size = fi.Size()
		// Write the header.
		if err := tw.WriteHeader(header); err != nil {
			return err
		}
		// Add the file to the archive.
		_, err = io.Copy(tw, sr)
		return err
	})
}

// managedTarSiaFiles creates a tarball from the renter's siafiles and writes
// it to dst.
func (r *Renter) managedTarSiaFiles(tw *tar.Writer) error {
	// Walk over all the siafiles and add them to the tarball.
	return filepath.Walk(r.staticFilesDir, func(path string, info os.FileInfo, err error) error {
		// This error is non-nil if filepath.Walk couldn't stat a file or
		// folder.
		if err != nil {
			return err
		}
		// Nothing to do for files not relevant to Sia.
		if !info.IsDir() &&
			filepath.Ext(path) != modules.SiaFileExtension &&
			filepath.Ext(path) != modules.SiaDirExtension &&
			filepath.Ext(path) != modules.PartialChunkExtension {
			return nil
		}
		// Create the header for the file/dir.
		header, err := tar.FileInfoHeader(info, info.Name())
		if err != nil {
			return err
		}
		relPath := strings.TrimPrefix(path, r.staticFilesDir)
		header.Name = relPath
		// If the info is a dir there is nothing more to do besides writing the
		// header.
		if info.IsDir() {
			return tw.WriteHeader(header)
		}
		// Handle siafiles and siadirs differently.
		var file io.Reader
		switch filepath.Ext(path) {
		case modules.SiaFileExtension:
			// Get the siafile.
			siaPathStr := strings.TrimSuffix(relPath, modules.SiaFileExtension)
			siaPathStr = strings.TrimSuffix(siaPathStr, modules.PartialsSiaFileExtension)
			siaPath, err := modules.NewSiaPath(siaPathStr)
			if err != nil {
				return err
			}
			entry, err := r.staticFileSet.Open(siaPath)
			if err != nil {
				return err
			}
			defer entry.Close()
			// Get a reader to read from the siafile.
			sr, err := entry.SnapshotReader()
			if err != nil {
				return err
			}
			defer sr.Close()
			file = sr
			// Update the size of the file within the header since it might have changed
			// while we weren't holding the lock.
			fi, err := sr.Stat()
			if err != nil {
				return err
			}
			header.Size = fi.Size()
		case modules.SiaDirExtension:
			// Get the siadir.
			var siaPath modules.SiaPath
			siaPathStr := strings.TrimSuffix(relPath, modules.SiaDirExtension)
			if siaPathStr == string(filepath.Separator) {
				siaPath = modules.RootSiaPath()
			} else {
				siaPath, err = modules.NewSiaPath(siaPathStr)
				if err != nil {
					return err
				}
			}
			entry, err := r.staticDirSet.Open(siaPath)
			if err != nil {
				return err
			}
			defer entry.Close()
			// Get a reader to read from the siafile.
			dr, err := entry.DirReader()
			if err != nil {
				return err
			}
			defer dr.Close()
			file = dr
			// Update the size of the file within the header since it might have changed
			// while we weren't holding the lock.
			fi, err := dr.Stat()
			if err != nil {
				return err
			}
			header.Size = fi.Size()
		}
		// Write the header.
		if err := tw.WriteHeader(header); err != nil {
			return err
		}
		// Add the file to the archive.
		_, err = io.Copy(tw, file)
		return err
	})
}

// managedUntarSiaDir untars a SiaDir from an archive and saves it to dst.
func (r *Renter) managedUntarSiaDir(b []byte, dst string) error {
	// Load the file as a .siadir
	var md siadir.Metadata
	err := json.Unmarshal(b, &md)
	if err != nil {
		return err
	}
	// Try creating a new SiaDir.
	var siaPath modules.SiaPath
	if err := siaPath.LoadSysPath(r.staticFilesDir, dst); err != nil {
		return err
	}
	siaPath, err = siaPath.Dir()
	if err != nil {
		return err
	}
	dirEntry, err := r.staticDirSet.NewSiaDir(siaPath)
	if err == siadir.ErrPathOverload {
		// .siadir exists already
		return nil
	} else if err != nil {
		return err // unexpected error
	}
	// Update the metadata.
	if err := dirEntry.UpdateMetadata(md); err != nil {
		dirEntry.Close()
		return err
	}
	if err := dirEntry.Close(); err != nil {
		return err
	}
	return nil
}

// managedUntarSiaFile untars a SiaFile from an archive and saves it to dst.
func (r *Renter) managedUntarSiaFile(b []byte, dst string, idxConversionMaps map[modules.ErasureCoderIdentifier]map[uint64]uint64) error {
	// Load the file as a SiaFile.
	reader := bytes.NewReader(b)
	sf, chunks, err := siafile.LoadSiaFileFromReaderWithChunks(reader, dst, r.wal)
	if err != nil {
		return err
	}
	// Use the conversion map to update the file's CombinedChunkIndex if
	// necessary.
	eci := sf.ErasureCode().Identifier()
	indexMap := idxConversionMaps[eci]
	if sf.CombinedChunkStatus() == siafile.CombinedChunkStatusCompleted {
		if indexMap == nil {
			return fmt.Errorf("expected indexMap for '%v' but couldn't find it", eci)
		}
		var newIndices []uint64
		for _, ci := range sf.CombinedChunkIndices() {
			newIndex, ok := indexMap[ci]
			if !ok {
				return fmt.Errorf("missing mapping for identifier '%v' at index '%v'", eci, ci)
			}
			newIndices = append(newIndices, newIndex)
		}
		sf.SetCombinedChunkIndices(newIndices)
	}
	// Add the file to the SiaFileSet.
	err = r.staticFileSet.AddExistingSiaFile(sf, chunks)
	if err != nil {
		return err
	}
	return nil
}

// managedUntarPartialsSiaFile untars a PartialsSiaFile from an archive and
// saves it to dst.
func (r *Renter) managedUntarPartialsSiaFile(b []byte, dst string, idxConversionMaps map[modules.ErasureCoderIdentifier]map[uint64]uint64) error {
	// Load the file as a SiaFile.
	psf, chunks, err := siafile.LoadSiaFileFromReaderWithChunks(bytes.NewReader(b), dst, r.wal)
	if err != nil {
		return err
	}
	// Add partial siafile to set.
	indexMap, err := r.staticFileSet.AddExistingPartialsSiaFile(psf, chunks)
	if err != nil {
		return err
	}
	// Remember indexMap to translate the combinedChunkIndex of imported
	// SiaFiles.
	eci := psf.ErasureCode().Identifier()
	if _, exists := idxConversionMaps[eci]; exists {
		return fmt.Errorf("idxConversionMaps already contains an entry for '%v' which shouldn't be the case", eci)
	}
	idxConversionMaps[eci] = indexMap
	return nil
}

// managedUntarPartialChunk untars a partial chunk from an archive and saves it
// to dst.
func (r *Renter) managedUntarPartialChunk(b []byte, dst string) error {
	// TODO: this is actually more tricky than this. There is a chance that the
	// partial chunk belongs to a siafile with a suffix. We need to figure out
	// how to handle that.
	f, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if os.IsExist(err) {
		return nil // partial chunk exists already
	} else if err != nil {
		return err
	}
	// Write partial chunk.
	_, err = f.Write(b)
	if err != nil {
		return errors.Compose(err, f.Close())
	}
	// Close file again.
	if err := f.Close(); err != nil {
		return err
	}
	return nil
}

// managedUntarDir untars the archive from src and writes the contents to dstFolder
// while preserving the relative paths within the archive.
func (r *Renter) managedUntarDir(tr *tar.Reader) error {
	// Copy the files from the tarball to the new location.
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		dst := filepath.Join(r.staticFilesDir, header.Name)

		// Check for dir.
		info := header.FileInfo()
		if info.IsDir() {
			if err = os.MkdirAll(dst, info.Mode()); err != nil {
				return err
			}
			continue
		}
		// Load the new file in memory.
		b, err := ioutil.ReadAll(tr)
		if err != nil {
			return err
		}
		// Save conversion maps required to map the CombinedChunkIndex of imported
		// Siafiles to their new CombinedChunkIndex.
		idxConversionMaps := make(map[modules.ErasureCoderIdentifier]map[uint64]uint64)

		switch filepath.Ext(info.Name()) {
		case modules.SiaDirExtension:
			err = r.managedUntarSiaDir(b, dst)
		case modules.SiaFileExtension:
			err = r.managedUntarSiaFile(b, dst, idxConversionMaps)
		case modules.PartialsSiaFileExtension:
			err = r.managedUntarPartialsSiaFile(b, dst, idxConversionMaps)
		case modules.PartialChunkExtension:
			err = r.managedUntarPartialChunk(b, dst)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// wrapReaderInCipher wraps the reader r into another reader according to the
// used encryption specified in the backupHeader.
func wrapReaderInCipher(r io.Reader, bh backupHeader, secret []byte) (io.Reader, error) {
	// Check if encryption is required and wrap the archive into a cipher if
	// necessary.
	switch bh.Encryption {
	case encryptionTwofish:
		c, err := twofish.NewCipher(secret)
		if err != nil {
			return nil, err
		}
		return cipher.StreamReader{
			S: cipher.NewCTR(c, bh.IV),
			R: r,
		}, nil
	case encryptionPlaintext:
		return r, nil
	default:
		return nil, errors.New("unknown cipher")
	}
}
