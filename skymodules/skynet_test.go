package skymodules

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"gitlab.com/NebulousLabs/errors"
	"gitlab.com/NebulousLabs/fastrand"
	"go.sia.tech/siad/crypto"
	"go.sia.tech/siad/persist"
	"go.sia.tech/siad/types"
)

// monetizationTestReader is a helper that allows for deterministically testing
// the monetization code.
type monetizationTestReader struct {
	n types.Currency
}

// newMonetizationReader creates a new monetization reader for testing.
func newMonetizationReader(n types.Currency) io.Reader {
	return &monetizationTestReader{n: n}
}

// Read will return bytes which when parsed as a currency result in 'n'.
func (r *monetizationTestReader) Read(b []byte) (int, error) {
	// Clear input.
	for i := range b {
		b[i] = 0
	}
	// Write n to b.
	nBytes := r.n.Big().Bytes()
	copy(b[len(b)-len(nBytes):], nBytes)
	return len(b), nil
}

// monetizationWalletTester is a helper that mocks a wallet and let's us check
// how SendSiacoinsMulti was called by the test.
type monetizationWalletTester struct {
	lastPayout []types.SiacoinOutput
}

// SendSiacoinsMulti implements the SiacoinSenderMulti interface.
func (w *monetizationWalletTester) SendSiacoinsMulti(outputs []types.SiacoinOutput) ([]types.Transaction, error) {
	w.lastPayout = outputs
	return nil, nil
}

// Reset sets the lastPayout to nil.
func (w *monetizationWalletTester) Reset() {
	w.lastPayout = nil
}

// newTestSkyfileLayout is a helper that returns a SkyfileLayout with some
// default settings for testing.
func newTestSkyfileLayout() SkyfileLayout {
	return SkyfileLayout{
		Version:            SkyfileVersion,
		Filesize:           1e6,
		MetadataSize:       14e3,
		FanoutSize:         75e3,
		FanoutDataPieces:   1,
		FanoutParityPieces: 10,
		CipherType:         crypto.TypePlain,
	}
}

// TestSkyfileLayoutEncoding checks that encoding and decoding a skyfile
// layout always results in the same struct.
func TestSkyfileLayoutEncoding(t *testing.T) {
	t.Parallel()
	// Try encoding an decoding a simple example.
	llOriginal := newTestSkyfileLayout()
	rand := fastrand.Bytes(64)
	copy(llOriginal.KeyData[:], rand)
	encoded := llOriginal.Encode()
	var llRecovered SkyfileLayout
	llRecovered.Decode(encoded)
	if llOriginal != llRecovered {
		t.Fatal("encoding and decoding of skyfileLayout does not match")
	}
}

// TestSkyfileLayout_DecodeFanoutIntoChunks verifies the functionality of
// 'DecodeFanoutIntoChunks' on the SkyfileLayout object.
func TestSkyfileLayout_DecodeFanoutIntoChunks(t *testing.T) {
	t.Parallel()

	// no bytes
	sl := newTestSkyfileLayout()
	chunks, err := sl.DecodeFanoutIntoChunks(make([]byte, 0))
	if chunks != nil || err != nil {
		t.Fatal("unexpected")
	}

	// not even number of chunks
	fanoutBytes := fastrand.Bytes(crypto.HashSize + 1)
	_, err = sl.DecodeFanoutIntoChunks(fanoutBytes)
	if err == nil || !strings.Contains(err.Error(), "the fanout bytes do not contain an even number of chunks") {
		t.Fatal("unexpected")
	}

	// valid fanout
	chunkSize := ChunkSize(sl.CipherType, uint64(sl.FanoutDataPieces))
	expectedChunks := sl.Filesize / chunkSize
	if sl.Filesize%chunkSize != 0 {
		expectedChunks++
	}
	fanoutBytes = fastrand.Bytes(int(expectedChunks * crypto.HashSize))
	chunks, err = sl.DecodeFanoutIntoChunks(fanoutBytes)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != int(expectedChunks) || len(chunks[0]) != 1 {
		t.Fatal("unexpected")
	}
	if !bytes.Equal(chunks[0][0][:], fanoutBytes[:crypto.HashSize]) {
		t.Fatal("unexpected")
	}

	// short fanout
	fanoutBytes = fastrand.Bytes(int(expectedChunks-1) * crypto.HashSize)
	chunks, err = sl.DecodeFanoutIntoChunks(fanoutBytes)
	if !errors.Contains(err, ErrMalformedBaseSector) {
		t.Fatal(err)
	}

	// long fanout
	fanoutBytes = fastrand.Bytes(int(expectedChunks+1) * crypto.HashSize)
	chunks, err = sl.DecodeFanoutIntoChunks(fanoutBytes)
	if !errors.Contains(err, ErrMalformedBaseSector) {
		t.Fatal(err)
	}

	// not 1-N
	sl.FanoutDataPieces = 4
	chunkSize = ChunkSize(sl.CipherType, uint64(sl.FanoutDataPieces))
	sl.Filesize = chunkSize * 3
	ppc := int(sl.FanoutDataPieces + sl.FanoutParityPieces) // pieces per chunk
	fanoutBytes = fastrand.Bytes(3 * ppc * crypto.HashSize) // 3 chunks
	chunks, err = sl.DecodeFanoutIntoChunks(fanoutBytes)
	if err != nil {
		t.Fatal("unexpected", err)
	}
	if len(chunks) != 3 || len(chunks[0]) != ppc {
		t.Fatal("unexpected")
	}
}

// TestSkyfileMetadata_ForPath tests the behaviour of the ForPath method.
func TestSkyfileMetadata_ForPath(t *testing.T) {
	filePath1 := "/foo/file1.txt"
	filePath2 := "/foo/file2.txt"
	filePath3 := "/file3.txt"
	filePath4 := "/bar/file4.txt"
	filePath5 := "/bar/baz/file5.txt"
	fullMeta := SkyfileMetadata{
		Subfiles: SkyfileSubfiles{
			filePath1: SkyfileSubfileMetadata{Filename: filePath1, Offset: 1, Len: 1},
			filePath2: SkyfileSubfileMetadata{Filename: filePath2, Offset: 2, Len: 2},
			filePath3: SkyfileSubfileMetadata{Filename: filePath3, Offset: 3, Len: 3},
			filePath4: SkyfileSubfileMetadata{Filename: filePath4, Offset: 4, Len: 4},
			filePath5: SkyfileSubfileMetadata{Filename: filePath5, Offset: 5, Len: 5},
		},
	}
	emptyMeta := SkyfileMetadata{}

	// Find an exact match.
	subMeta, isSubFile, offset, size := fullMeta.ForPath(filePath1)
	if _, exists := subMeta.Subfiles[filePath1]; !exists {
		t.Fatal("Expected to find a file by its full path and name.")
	}
	if !isSubFile {
		t.Fatal("Expected to find a file, got a dir.")
	}
	if offset != 1 {
		t.Fatalf("Expected offset %d, got %d", 1, offset)
	}
	if size != 1 {
		t.Fatalf("Expected size %d, got %d", 1, size)
	}

	// Find files by their directory.
	subMeta, isSubFile, offset, size = fullMeta.ForPath("/foo")
	subfile1, exists1 := subMeta.Subfiles[filePath1]
	subfile2, exists2 := subMeta.Subfiles[filePath2]
	// Expect to find files 1 and 2 and nothing else.
	if !(exists1 && exists2 && len(subMeta.Subfiles) == 2) {
		t.Fatal("Expected to find two files by their directory.")
	}
	if isSubFile {
		t.Fatal("Expected to find a dir, got a file.")
	}
	if offset != 1 {
		t.Fatalf("Expected offset %d, got %d", 1, offset)
	}
	if subfile1.Offset != 0 {
		t.Fatalf("Expected offset %d, got %d", 0, subfile1.Offset)
	}
	if subfile2.Offset != 1 {
		t.Fatalf("Expected offset %d, got %d", 1, subfile2.Offset)
	}

	if size != 3 {
		t.Fatalf("Expected size %d, got %d", 3, size)
	}

	// Find files in the given directory and its subdirectories.
	subMeta, isSubFile, offset, size = fullMeta.ForPath("/bar")
	subfile4, exists4 := subMeta.Subfiles[filePath4]
	subfile5, exists5 := subMeta.Subfiles[filePath5]
	// Expect to find files 4 and 5 and nothing else.
	if !(exists4 && exists5 && len(subMeta.Subfiles) == 2) {
		t.Fatal("Expected to find two files by their directory.")
	}
	if isSubFile {
		t.Fatal("Expected to find a dir, got a file.")
	}
	if offset != 4 {
		t.Fatalf("Expected offset %d, got %d", 4, offset)
	}
	if subfile4.Offset != 0 {
		t.Fatalf("Expected offset %d, got %d", 0, subfile4.Offset)
	}
	if subfile5.Offset != 1 {
		t.Fatalf("Expected offset %d, got %d", 1, subfile5.Offset)
	}
	if size != 9 {
		t.Fatalf("Expected size %d, got %d", 9, size)
	}

	// Find files in the given directory.
	subMeta, isSubFile, offset, size = fullMeta.ForPath("/bar/baz/")
	subfile5, exists5 = subMeta.Subfiles[filePath5]
	// Expect to find file 5 and nothing else.
	if !(exists5 && len(subMeta.Subfiles) == 1) {
		t.Fatal("Expected to find one file by its directory.")
	}
	if isSubFile {
		t.Fatal("Expected to find a dir, got a file.")
	}
	if offset != 5 {
		t.Fatalf("Expected offset %d, got %d", 5, offset)
	}
	if subfile5.Offset != 0 {
		t.Fatalf("Expected offset %d, got %d", 0, subfile4.Offset)
	}
	if size != 5 {
		t.Fatalf("Expected size %d, got %d", 5, size)
	}

	// Expect no files found on nonexistent path.
	for _, path := range []string{"/nonexistent", "/fo", "/file", "/b", "/bar/ba"} {
		subMeta, _, offset, size = fullMeta.ForPath(path)
		if len(subMeta.Subfiles) > 0 {
			t.Fatalf("Expected to not find any files on nonexistent path %s but found %v", path, len(subMeta.Subfiles))
		}
		if offset != 0 {
			t.Fatalf("Expected offset %d, got %d", 0, offset)
		}
		if size != 0 {
			t.Fatalf("Expected size %d, got %d", 0, size)
		}
	}

	// Find files by their directory, even if it contains a trailing slash.
	subMeta, _, _, _ = fullMeta.ForPath("/foo/")
	if _, exists := subMeta.Subfiles[filePath1]; !exists {
		t.Fatal(`Expected to find a file by its directory, even when followed by a "/".`)
	}
	subMeta, _, _, _ = fullMeta.ForPath("foo/")
	if _, exists := subMeta.Subfiles[filePath1]; !exists {
		t.Fatal(`Expected to find a file by its directory, even when missing leading "/" and followed by a "/".`)
	}

	// Find files by their directory, even if it's missing its leading slash.
	subMeta, _, _, _ = fullMeta.ForPath("foo")
	if _, exists := subMeta.Subfiles[filePath1]; !exists {
		t.Fatal(`Expected to find a file by its directory, even when it's missing its leading "/".`)
	}

	// Try to find a file in an empty metadata struct.
	subMeta, _, offset, _ = emptyMeta.ForPath("foo")
	if len(subMeta.Subfiles) != 0 {
		t.Fatal(`Expected to not find any files, found`, len(subMeta.Subfiles))
	}
	if offset != 0 {
		t.Fatal(`Expected offset to be zero, got`, offset)
	}
}

// TestSkyfileMetadata_IsDirectory is a table test for the IsDirectory method.
func TestSkyfileMetadata_IsDirectory(t *testing.T) {
	tests := []struct {
		name           string
		meta           SkyfileMetadata
		expectedResult bool
	}{
		{
			name: "nil subfiles",
			meta: SkyfileMetadata{
				Filename: "foo",
				Length:   10,
				Mode:     0644,
				Subfiles: nil,
			},
			expectedResult: false,
		},
		{
			name: "empty subfiles struct",
			meta: SkyfileMetadata{
				Filename: "foo",
				Length:   10,
				Mode:     0644,
				Subfiles: SkyfileSubfiles{},
			},
			expectedResult: false,
		},
		{
			name: "one subfile",
			meta: SkyfileMetadata{
				Filename: "foo",
				Length:   10,
				Mode:     0644,
				Subfiles: SkyfileSubfiles{
					"foo": SkyfileSubfileMetadata{
						FileMode:    10,
						Filename:    "foo",
						ContentType: "text/plain",
						Offset:      0,
						Len:         10,
					},
				},
			},
			expectedResult: false,
		},
		{
			name: "two subfiles",
			meta: SkyfileMetadata{
				Filename: "foo",
				Length:   20,
				Mode:     0644,
				Subfiles: SkyfileSubfiles{
					"foo": SkyfileSubfileMetadata{
						FileMode:    10,
						Filename:    "foo",
						ContentType: "text/plain",
						Offset:      0,
						Len:         10,
					},
					"bar": SkyfileSubfileMetadata{
						FileMode:    10,
						Filename:    "bar",
						ContentType: "text/plain",
						Offset:      10,
						Len:         10,
					},
				},
			},
			expectedResult: true,
		},
	}

	for _, test := range tests {
		result := test.meta.IsDirectory()
		if result != test.expectedResult {
			t.Fatalf("'%s' failed: expected '%t', got '%t'", test.name, test.expectedResult, result)
		}
	}
}

// TestComputeMonetizationPayout is a unit test for ComputeMonetizationPayout.
func TestComputeMonetizationPayout(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	t.Parallel()

	tests := []struct {
		amt    uint64
		base   uint64
		n      uint64
		payout uint64
	}{
		{
			// Don't pay if amt is 0.
			amt:    0,
			base:   1,
			payout: 0,
		},
		{
			// Pay if amt and base are the same.
			amt:    1,
			base:   1,
			payout: 1,
		},
		{
			// Pay if amt > base.
			amt:    2,
			base:   1,
			payout: 2,
		},
		{
			// 50% chance - drawing 0 should pass.
			amt:    1,
			base:   2,
			n:      0,
			payout: 2,
		},
		{
			// 50% chance - drawing 50 should fail.
			amt:    1,
			base:   2,
			n:      1,
			payout: 0,
		},
		{
			// 50% chance - drawing 2 should pass.
			// 2 mod 2 == 0 mod 2
			amt:    1,
			base:   2,
			n:      2,
			payout: 2,
		},
		{
			// 99% chance - drawing 98 should pass.
			amt:    99,
			base:   100,
			n:      98,
			payout: 100,
		},
		{
			// 99% chance - drawing 99 should fail.
			amt:    99,
			base:   100,
			n:      99,
			payout: 0,
		},
		{
			// 99% chance - drawing 100 should pass.
			// 100 mod 100 == 0
			amt:    99,
			base:   100,
			n:      100,
			payout: 100,
		},
		{
			// 99% chance - drawing 1 should pass.
			amt:    99,
			base:   100,
			n:      1,
			payout: 100,
		},
	}

	for i, test := range tests {
		amt := types.NewCurrency64(test.amt)
		base := types.NewCurrency64(test.base)
		rand := newMonetizationReader(types.NewCurrency64(test.n))
		p, err := computeMonetizationPayout(amt, base, rand)
		if err != nil {
			t.Fatal(err)
		}
		payout, err := p.Uint64()
		if err != nil {
			t.Fatal(err)
		}
		if payout != test.payout {
			t.Log("amt", test.amt)
			t.Log("base", test.base)
			t.Log("n", test.n)
			t.Log("expected payout", test.payout)
			t.Log("actual pay", payout)
			t.Fatalf("Test %v failed", i)
		}
	}

	// Do a run with an actual random number generator and a 30% chance.
	payout := types.ZeroCurrency
	nTotal := uint64(500000)
	amt := types.NewCurrency64(30)
	base := types.NewCurrency64(100)
	for i := uint64(0); i < nTotal; i++ {
		p := ComputeMonetizationPayout(amt, base)
		payout = payout.Add(p)
	}

	// The payout should be off by less than 1%.
	idealPayout := types.NewCurrency64(nTotal).Mul(amt)
	tolerance := idealPayout.MulFloat(0.01)

	var diff types.Currency
	if idealPayout.Cmp(payout) > 0 {
		diff = idealPayout.Sub(payout)
	} else {
		diff = payout.Sub(idealPayout)
	}
	if diff.Cmp(tolerance) > 0 {
		t.Fatal("diff exceeds the 1% tolerance", diff, tolerance)
	}

	t.Log("Total executions:", nTotal)
	t.Log("Ideal payout:    ", idealPayout)
	t.Log("Actual payout:   ", payout)

	// Check critical for 0 base.
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("build.Critical wasn't triggered")
		}
	}()
	ComputeMonetizationPayout(types.NewCurrency64(1), types.ZeroCurrency)
}

// TestPayMonetizers is a unit test for payMonetizers.
func TestPayMonetizers(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	t.Parallel()

	// Create test wallet.
	w := &monetizationWalletTester{}

	// Monetization base..
	base := types.SiacoinPrecision.Mul64(1000) // 1KS payouts

	// Declare a helper to create valid monetizers.
	validMonetization := func() *Monetization {
		m := &Monetization{
			License: LicenseMonetization,
			Monetizers: []Monetizer{
				{
					Amount:   types.SiacoinPrecision.Div64(100000), // $0.00001
					Currency: CurrencyUSD,
				},
			},
		}
		fastrand.Read(m.Monetizers[0].Address[:])
		return m
	}

	// Declare a test helper.
	test := func(rate types.Currency) {
		conversionRates := map[string]types.Currency{
			CurrencyUSD: rate,
		}

		// no data
		m := validMonetization()
		err := PayMonetizers(w, m, 0, 100, conversionRates, base)
		if err != nil {
			t.Fatal(err)
		}

		// invalid currency
		m = validMonetization()
		m.Monetizers[0].Currency = ""
		err = PayMonetizers(w, m, 100, 100, conversionRates, base)
		if !errors.Contains(err, ErrInvalidCurrency) {
			t.Fatal(err)
		}

		// Run the remaining test 1000 times to make sure the success test cases
		// don't just pass by accident.
		for i := 0; i < 1000; i++ {
			// pay out base - lottery success
			m = validMonetization()
			n := m.Monetizers[0].Amount.Mul(rate).Div(types.SiacoinPrecision).Sub64(1) // n < amt -> success
			err = payMonetizers(w, m, 100, 100, conversionRates, base, newMonetizationReader(n))
			if err != nil {
				t.Fatal(err)
			}
			// there should be 1 payout of amount 'base' to the right address.
			if len(w.lastPayout) != 1 {
				t.Fatal("wrong number of payouts", len(w.lastPayout))
			}
			if !w.lastPayout[0].Value.Equals(base) {
				t.Fatal("wrong payout amount", w.lastPayout[0].Value, base)
			}
			if w.lastPayout[0].UnlockHash != m.Monetizers[0].Address {
				t.Fatal("wrong payout address")
			}
			w.Reset()

			// pay out base - lottery failure
			m = validMonetization()
			n = m.Monetizers[0].Amount.Mul(rate).Div(types.SiacoinPrecision) // n == amt -> failure
			err = payMonetizers(w, m, 100, 100, conversionRates, base, newMonetizationReader(n))
			if err != nil {
				t.Fatal(err)
			}
			// there should be no payout
			if len(w.lastPayout) != 0 {
				t.Fatal("wrong number of payouts", len(w.lastPayout))
			}
			w.Reset()
		}
	}

	// Run the test twice. Once with a positive and once with a negative
	// conversion rate.
	test(types.SiacoinPrecision.Mul64(10)) // $1 = 10SC
	test(types.SiacoinPrecision.Div64(10)) // $1 = 0.1SC

	// Make sure a 0 base or conversion rate is not accepted.
	m := validMonetization()
	conversionRates := map[string]types.Currency{
		CurrencyUSD: types.SiacoinPrecision,
	}
	err := PayMonetizers(w, m, 100, 100, conversionRates, types.ZeroCurrency)
	if !errors.Contains(err, ErrZeroBase) {
		t.Fatal(err)
	}
	conversionRates = map[string]types.Currency{
		CurrencyUSD: types.ZeroCurrency,
	}
	err = PayMonetizers(w, m, 100, 100, conversionRates, base)
	if !errors.Contains(err, ErrZeroConversionRate) {
		t.Fatal(err)
	}
}

// TestIsSkynetDir probes the IsSkynetDir function
func TestIsSkynetDir(t *testing.T) {
	// Define tests
	var tests = []struct {
		sp  SiaPath
		res bool
	}{
		// Valid Cases
		{SkynetFolder, true},
		{NewGlobalSiaPath("/var/skynet"), true},
		{NewGlobalSiaPath("/var/skynet/skynet"), true},
		{NewGlobalSiaPath("/var/skynet/" + persist.RandomSuffix()), true},

		// Invalid Cases
		{VarFolder, false},
		{HomeFolder, false},
		{UserFolder, false},
		{BackupFolder, false},
		{RootSiaPath(), false},
		{NewGlobalSiaPath("/home/var/skynet"), false},
		{NewGlobalSiaPath("/var"), false},
		{NewGlobalSiaPath("/skynet"), false},
	}
	// Execute tests
	for _, test := range tests {
		if IsSkynetDir(test.sp) != test.res {
			t.Error("unexpected", test)
		}
	}
}

// TestDeterminePathBasedOnTryFiles makes sure we make the right decisions
// when choosing paths.
func TestDeterminePathBasedOnTryFiles(t *testing.T) {
	t.Parallel()

	tfWithGlobalIndex := []string{"good-news/index.html", "index.html", "/index.html"}
	tfNoGlobalIndex := []string{"index.html", "good-news/index.html"}
	tfNoIndex := []string{"good-news/index.html"}

	subfiles := SkyfileSubfiles{
		"index.html": SkyfileSubfileMetadata{
			Filename: "index.html",
		},
		"404.html": SkyfileSubfileMetadata{
			Filename: "404.html",
		},
		"about/index.html": SkyfileSubfileMetadata{
			Filename: "about/index.html",
		},
		"news/good-news/index.html": SkyfileSubfileMetadata{
			Filename: "news/good-news/index.html",
		},
		"img/image.html": SkyfileSubfileMetadata{
			Filename: "img/image.html",
		},
	}

	tests := []struct {
		name         string
		tryfiles     []string
		requestPath  string
		expectedPath string
	}{
		// Global index
		{
			name:         "global index, request path ''",
			tryfiles:     tfWithGlobalIndex,
			requestPath:  "",
			expectedPath: "/index.html",
		},
		{
			name:         "global index, request path '/about'",
			tryfiles:     tfWithGlobalIndex,
			requestPath:  "/about",
			expectedPath: "/about/index.html",
		},
		{
			name:         "global index, request path '/news/noexist.html'",
			tryfiles:     tfWithGlobalIndex,
			requestPath:  "/news/noexist.html",
			expectedPath: "/index.html",
		},
		{
			name:         "global index, request path '/news/bad-news'",
			tryfiles:     tfWithGlobalIndex,
			requestPath:  "/news/bad-news",
			expectedPath: "/index.html",
		},
		{
			name:         "global index, request path '/news/good-news'",
			tryfiles:     tfWithGlobalIndex,
			requestPath:  "/news/good-news",
			expectedPath: "/news/good-news/index.html",
		},
		{
			name:         "global index, request path '/img/noexist.png'",
			tryfiles:     tfWithGlobalIndex,
			requestPath:  "/img/noexist.png",
			expectedPath: "/index.html",
		},
		// No global index
		{
			name:         "no global index, request path ''",
			tryfiles:     tfNoGlobalIndex,
			requestPath:  "",
			expectedPath: "/index.html",
		},
		{
			name:         "no global index, request path '/index.html'",
			tryfiles:     tfNoGlobalIndex,
			requestPath:  "/index.html",
			expectedPath: "/index.html",
		},
		{
			name:         "no global index, request path '/about'",
			tryfiles:     tfNoGlobalIndex,
			requestPath:  "/about",
			expectedPath: "/about/index.html",
		},
		{
			name:         "no global index, request path '/about/index.html'",
			tryfiles:     tfNoGlobalIndex,
			requestPath:  "/about/index.html",
			expectedPath: "/about/index.html",
		},
		{
			name:         "no global index, request path '/news/noexist.html'",
			tryfiles:     tfNoGlobalIndex,
			requestPath:  "/news/noexist.html",
			expectedPath: "/news/noexist.html",
		},
		{
			name:         "no global index, request path '/news/bad-news'",
			tryfiles:     tfNoGlobalIndex,
			requestPath:  "/news/bad-news",
			expectedPath: "/news/bad-news",
		},
		{
			name:         "no global index, request path '/news/good-news'",
			tryfiles:     tfNoGlobalIndex,
			requestPath:  "/news/good-news",
			expectedPath: "/news/good-news/index.html",
		},
		// No index
		{
			name:         "no index, request path ''",
			requestPath:  "",
			expectedPath: "",
		},
		{
			name:         "no index, request path '/index.html'",
			tryfiles:     tfNoIndex,
			requestPath:  "/index.html",
			expectedPath: "/index.html",
		},
		{
			name:         "no index, request path '/about'",
			tryfiles:     tfNoIndex,
			requestPath:  "/about",
			expectedPath: "/about",
		},
		{
			name:         "no index, request path '/about/index.html'",
			tryfiles:     tfNoIndex,
			requestPath:  "/about/index.html",
			expectedPath: "/about/index.html",
		},
		{
			name:         "no index, request path '/news/noexist.html'",
			tryfiles:     tfNoIndex,
			requestPath:  "/news/noexist.html",
			expectedPath: "/news/noexist.html",
		},
		{
			name:         "no index, request path '/news/bad-news'",
			tryfiles:     tfNoIndex,
			requestPath:  "/news/bad-news",
			expectedPath: "/news/bad-news",
		},
		{
			name:         "no index, request path '/news'",
			tryfiles:     tfNoIndex,
			requestPath:  "/news",
			expectedPath: "/news/good-news/index.html",
		},
		{
			name:         "no index, request path '/news/good-news'",
			tryfiles:     tfNoIndex,
			requestPath:  "/news/good-news",
			expectedPath: "/news/good-news",
		},
	}

	meta := SkyfileMetadata{
		TryFiles: tfWithGlobalIndex,
	}
	path := meta.determinePathBasedOnTryfiles("anypath")
	if path != "anypath" {
		t.Fatalf("Expected path to be 'anypath', got '%s'", path)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			meta := SkyfileMetadata{
				Subfiles: subfiles,
				TryFiles: tt.tryfiles,
			}
			path = meta.determinePathBasedOnTryfiles(tt.requestPath)
			if path != tt.expectedPath {
				t.Log("Test name:", tt.name)
				t.Fatalf("Expected path to be '%s', got '%s'", tt.expectedPath, path)
			}
		})
	}
}
