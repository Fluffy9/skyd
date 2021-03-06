package main

import (
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"gitlab.com/SkynetLabs/skyd/build"
)

// TestRootSkycCmd tests root siac command for expected outputs. The test
// runs its own node and requires no service running at port 5555.
func TestRootSkycCmd(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	t.Parallel()

	// Create a test node for this test group
	groupDir := skycTestDir(t.Name())
	n, err := newTestNode(groupDir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := n.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	// Initialize siac root command with its subcommands and flags
	root := getRootCmdForSkycCmdsTests(groupDir)

	// define test constants:
	// Regular expressions to check siac output

	begin := "^"
	nl := `
` // platform agnostic new line
	end := "$"

	// Capture root command usage for test comparison
	// catch stdout and stderr
	rootCmdUsagePattern := getCmdUsage(t, root)

	IPv6addr := n.Address
	IPv4Addr := strings.ReplaceAll(n.Address, "[::]", "localhost")

	// Regex helpers
	// \s+ to match 1 or more spaces. Helpful with the tab writter
	// \d+ to match a number
	rootCmdOutPattern := `Consensus:
  Synced: (No|Yes)
  Height:\s+\d+

Wallet:
(  Status: Locked|  Status:          unlocked
  Siacoin Balance: \d+(\.\d*|) (SC|KS|MS))

Renter:
File Summary:
  Files:\s+\d+
  Total Stored:\s+\d+(\.\d+|) ( B|kB|MB|GB|TB)
  Total Renewing Data:\s+\d+(\.\d+|) ( B|kB|MB|GB|TB)
Repair Status:
  Last Health Check:\s+\d+(m)
  Repair Data Remaining:\s+\d+(\.\d+|) ( B|kB|MB|GB|TB)
  Stuck Repair Remaining:\s+\d+(\.\d+|) ( B|kB|MB|GB|TB)
  Stuck Chunks:\s+\d+
  Max Health:\s+\d+(\%)
  Min Redundancy:\s+(\d+.\d{2}|-)
  Lost Files:\s+\d+
Contract Summary:
  Active Contracts:\s+\d+
  Passive Contracts:\s+\d+
  Disabled Contracts:\s+\d+`

	rootCmdVerbosePartPattern := `Global Rate limits: 
  Download Speed: (no limit|\d+(\.\d+)? (B/s|KB/s|MB/s|GB/s|TB/s))
  Upload Speed:   (no limit|\d+(\.\d+)? (B/s|KB/s|MB/s|GB/s|TB/s))

Gateway Rate limits: 
  Download Speed: (no limit|\d+(\.\d+)? (B/s|KB/s|MB/s|GB/s|TB/s))
  Upload Speed:   (no limit|\d+(\.\d+)? (B/s|KB/s|MB/s|GB/s|TB/s))

Renter Rate limits: 
  Download Speed: (no limit|\d+(\.\d+)? (B/s|KB/s|MB/s|GB/s|TB/s))
  Upload Speed:   (no limit|\d+(\.\d+)? (B/s|KB/s|MB/s|GB/s|TB/s))`

	connectionRefusedPattern := `Could not get consensus status: \[failed to get reader response; GET request failed; Get "?http://127.0.0.1:5555/consensus"?: dial tcp 127.0.0.1:5555: connect: connection refused\]`
	versionReplacer := strings.NewReplacer(".", `\.`, "?", `\?`)
	skyClientVersionPattern := "Skynet Client v" + versionReplacer.Replace(build.NodeVersion)

	// Define subtests
	// We can't test siad on default address (port) when test node has
	// dynamically allocated port, we have to use node address.
	subTests := []skycCmdSubTest{
		{
			name:               "TestRootCmdWithShortAddressFlagIPv6",
			test:               testGenericSkycCmd,
			cmd:                root,
			cmdStrs:            []string{"-a", IPv6addr},
			expectedOutPattern: begin + rootCmdOutPattern + nl + nl + end,
		},
		{
			name:               "TestRootCmdWithShortAddressFlagIPv4",
			test:               testGenericSkycCmd,
			cmd:                root,
			cmdStrs:            []string{"-a", IPv4Addr},
			expectedOutPattern: begin + rootCmdOutPattern + nl + nl + end,
		},
		{
			name:               "TestRootCmdWithLongAddressFlagIPv6",
			test:               testGenericSkycCmd,
			cmd:                root,
			cmdStrs:            []string{"--addr", IPv6addr},
			expectedOutPattern: begin + rootCmdOutPattern + nl + nl + end,
		},
		{
			name:               "TestRootCmdWithLongAddressFlagIPv4",
			test:               testGenericSkycCmd,
			cmd:                root,
			cmdStrs:            []string{"--addr", IPv4Addr},
			expectedOutPattern: begin + rootCmdOutPattern + nl + nl + end,
		},
		{
			name:               "TestRootCmdWithVerboseFlag",
			test:               testGenericSkycCmd,
			cmd:                root,
			cmdStrs:            []string{"--addr", IPv4Addr, "-v"},
			expectedOutPattern: begin + rootCmdOutPattern + nl + nl + rootCmdVerbosePartPattern + nl + nl + end,
		},
		{
			name:               "TestRootCmdWithInvalidFlag",
			test:               testGenericSkycCmd,
			cmd:                root,
			cmdStrs:            []string{"-x"},
			expectedOutPattern: begin + "Error: unknown shorthand flag: 'x' in -x" + nl + rootCmdUsagePattern + nl + end,
		},
		{
			name:               "TestRootCmdWithInvalidAddress",
			test:               testGenericSkycCmd,
			cmd:                root,
			cmdStrs:            []string{"-a", "127.0.0.1:5555"},
			expectedOutPattern: begin + connectionRefusedPattern + nl + nl + end,
		},
		{
			name:               "TestRootCmdWithHelpFlag",
			test:               testGenericSkycCmd,
			cmd:                root,
			cmdStrs:            []string{"-h"},
			expectedOutPattern: begin + skyClientVersionPattern + nl + nl + rootCmdUsagePattern + end,
		},
	}

	// run tests
	err = runSkycCmdSubTests(t, subTests)
	if err != nil {
		t.Fatal(err)
	}
}

// getCmdUsage gets root command usage regex pattern by calling usage function
func getCmdUsage(t *testing.T, cmd *cobra.Command) string {
	// Capture usage by calling a usage function
	c, err := newOutputCatcher()
	if err != nil {
		t.Fatal("Error starting catching stdout/stderr", err)
	}
	usageFunc := cmd.UsageFunc()
	err = usageFunc(cmd)
	if err != nil {
		t.Fatal("Error getting reference root siac usage", err)
	}
	baseUsage, err := c.stop()

	// Escape regex special chars
	usage := escapeRegexChars(baseUsage)

	// Inject 2 missing rows
	beforeHelpCommand := "Perform gateway actions"
	helpCommand := "  help        Help about any command"
	nl := `
`
	usage = strings.ReplaceAll(usage, beforeHelpCommand, beforeHelpCommand+nl+helpCommand)
	beforeHelpFlag := "the password for the API's http authentication"
	helpFlag := `  -h, --help                   help for .*skyc(\.test|)`
	cmdUsagePattern := strings.ReplaceAll(usage, beforeHelpFlag, beforeHelpFlag+nl+helpFlag)

	return cmdUsagePattern
}
