package mdm

import (
	"testing"

	"gitlab.com/NebulousLabs/Sia/crypto"
	"gitlab.com/NebulousLabs/Sia/types"
	"gitlab.com/NebulousLabs/fastrand"
	"gitlab.com/skynetlabs/skyd/skymodules"
)

// TestInstructionReadOffset tests executing a program with a single
// ReadOffsetInstruction.
func TestInstructionReadOffset(t *testing.T) {
	host := newTestHost()
	mdm := New(host)
	defer mdm.Stop()

	// Prepare a priceTable.
	pt := newTestPriceTable()
	duration := types.BlockHeight(fastrand.Uint64n(5))
	// Prepare storage obligation.
	so := host.newTestStorageObligation(true)
	so.AddRandomSectors(3)
	root := so.sectorRoots[1] // middle sector
	outputData, err := host.ReadSector(root)
	if err != nil {
		t.Fatal(err)
	}
	// Use a builder to build the program.
	tb := newTestProgramBuilder(pt, duration)
	tb.AddReadOffsetInstruction(skymodules.SectorSize, skymodules.SectorSize, true)

	ics := so.ContractSize()
	imr := so.MerkleRoot()

	// Execute it.
	outputs, err := mdm.ExecuteProgramWithBuilder(tb, so, duration, false)
	if err != nil {
		t.Fatal(err)
	}

	// Compute the expected proof. It's a regular range proof since we proof the
	// whole sector.
	expectedProof := crypto.MerkleSectorRangeProof(so.sectorRoots, int(1), int(2))

	// Assert the output.
	err = outputs[0].assert(ics, imr, expectedProof, outputData, nil)
	if err != nil {
		t.Fatal(err)
	}
	sectorData := outputs[0].Output

	// Verify the proof.
	proofStart := int(skymodules.SectorSize) / crypto.SegmentSize
	proofEnd := int(skymodules.SectorSize*2) / crypto.SegmentSize
	ok := crypto.VerifyMixedRangeProof(outputs[0].Output, outputs[0].Proof, outputs[0].NewMerkleRoot, proofStart, proofEnd)
	if !ok {
		t.Fatal("failed to verify proof")
	}

	// Create a program to read up to half a sector from the host.
	offset := skymodules.SectorSize + skymodules.SectorSize/2 // start in the middle of the middle sector
	relOffset := skymodules.SectorSize / 2
	// Read half a sector.
	numSegments := skymodules.SectorSize / 2 / crypto.SegmentSize
	length := numSegments * crypto.SegmentSize

	// Use a builder to build the program.
	tb = newTestProgramBuilder(pt, duration)
	tb.AddReadOffsetInstruction(length, offset, true)

	// Execute it.
	outputs, err = mdm.ExecuteProgramWithBuilder(tb, so, duration, false)
	if err != nil {
		t.Fatal(err)
	}

	// Assert the output.
	proofStart = int(offset) / crypto.SegmentSize
	proofEnd = int(offset+length) / crypto.SegmentSize
	sectorProof := expectedProof
	expectedProof = crypto.MerkleMixedRangeProof(sectorProof, sectorData, int(skymodules.SectorSize), proofStart, proofEnd)
	outputData = sectorData[relOffset:][:length]
	err = outputs[0].assert(ics, imr, expectedProof, outputData, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Verify proof.
	ok = crypto.VerifyMixedRangeProof(outputs[0].Output, outputs[0].Proof, outputs[0].NewMerkleRoot, proofStart, proofEnd)
	if !ok {
		t.Fatal("failed to verify mixed range proof")
	}
}