package mdm

import (
	"testing"

	"gitlab.com/NebulousLabs/Sia/crypto"
	"gitlab.com/NebulousLabs/Sia/modules"
	"gitlab.com/NebulousLabs/Sia/types"
	"gitlab.com/NebulousLabs/fastrand"
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
	so := newTestStorageObligation(true)
	so.sectorRoots = randomSectorRoots(initialContractSectors)
	root := so.sectorRoots[0]
	outputData, err := host.ReadSector(root)
	if err != nil {
		t.Fatal(err)
	}
	// Use a builder to build the program.
	readLen := modules.SectorSize
	tb := newTestProgramBuilder(pt, duration)
	tb.AddReadOffsetInstruction(readLen, 0, true)

	ics := so.ContractSize()
	imr := so.MerkleRoot()

	// Execute it.
	outputs, err := mdm.ExecuteProgramWithBuilder(tb, so, duration, false)
	if err != nil {
		t.Fatal(err)
	}

	// Assert the output.
	err = outputs[0].assert(ics, imr, []crypto.Hash{}, outputData)
	if err != nil {
		t.Fatal(err)
	}
	sectorData := outputs[0].Output

	// Create a program to read half a sector from the host.
	offset := modules.SectorSize / 2                     // start in the middle
	length := fastrand.Uint64n(modules.SectorSize/2) + 1 // up to half a sector
	if mod := length % crypto.SegmentSize; mod != 0 {    // must be multiple of segmentsize
		length -= mod
	}
	if length == 0 {
		length = crypto.SegmentSize
	}

	// Use a builder to build the program.
	tb = newTestProgramBuilder(pt, duration)
	tb.AddReadOffsetInstruction(length, offset, true)

	// Execute it.
	outputs, err = mdm.ExecuteProgramWithBuilder(tb, so, duration, false)
	if err != nil {
		t.Fatal(err)
	}

	// Assert the output.
	proofStart := int(offset) / crypto.SegmentSize
	proofEnd := int(offset+length) / crypto.SegmentSize
	proof := crypto.MerkleRangeProof(sectorData, proofStart, proofEnd)
	outputData = sectorData[offset:][:length]
	err = outputs[0].assert(ics, imr, proof, outputData)
	if err != nil {
		t.Fatal(err)
	}
}
