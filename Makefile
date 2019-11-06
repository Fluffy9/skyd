# These variables get inserted into ./build/commit.go
BUILD_TIME=$(shell date)
GIT_REVISION=$(shell git rev-parse --short HEAD)
GIT_DIRTY=$(shell git diff-index --quiet HEAD -- || echo "✗-")

ldflags= -X gitlab.com/NebulousLabs/Sia/build.GitRevision=${GIT_DIRTY}${GIT_REVISION} \
-X "gitlab.com/NebulousLabs/Sia/build.BuildTime=${BUILD_TIME}"

# all will build and install release binaries
all: release

# pkgs changes which packages the makefile calls operate on. run changes which
# tests are run during testing.
run = .
cpkg = ./modules/renter
pkgs = ./build ./cmd/siac ./cmd/siad ./compatibility ./crypto ./encoding ./modules ./modules/consensus ./modules/explorer  \
       ./modules/gateway ./modules/host ./modules/host/contractmanager ./modules/renter ./modules/renter/contractor        \
       ./modules/renter/hostdb ./modules/renter/hostdb/hosttree ./modules/renter/proto ./modules/renter/siadir             \
       ./modules/renter/siafile ./modules/miner ./modules/wallet ./modules/transactionpool ./node ./node/api ./persist     \
       ./siatest ./siatest/consensus ./siatest/daemon ./siatest/gateway ./siatest/host ./siatest/miner ./siatest/renter    \
       ./siatest/renter/contractor ./siatest/renter/hostdb ./siatest/renterhost ./siatest/transactionpool ./siatest/wallet \
       ./node/api/server ./sync ./types ./types/typesutil
utils = ./cmd/sia-node-scanner 

# fmt calls go fmt on all packages and utils.
fmt:
	gofmt -s -l -w $(pkgs)
	gofmt -s -l -w $(utils)

# vet calls go vet on all packages and utils.
# NOTE: go vet requires packages to be built in order to obtain type info.
vet:
	GO111MODULE=on go vet $(pkgs)
	GO111MODULE=on go vet $(utils)

lint:
	go get golang.org/x/lint/golint
	golint -min_confidence=1.0 -set_exit_status $(pkgs)
	golint -min_confidence=1.0 -set_exit_status $(utils)

# spellcheck checks for misspelled words in comments or strings.
spellcheck:
	misspell -error .

# debug builds and installs debug binaries.
debug:
	GO111MODULE=on go install -tags='debug profile netgo' -ldflags='$(ldflags)' $(pkgs)
debug-race:
	GO111MODULE=on go install -race -tags='debug profile netgo' -ldflags='$(ldflags)' $(pkgs)

# dev builds and installs developer binaries.
dev:
	GO111MODULE=on go install -tags='dev debug profile netgo' -ldflags='$(ldflags)' $(pkgs)
dev-race:
	GO111MODULE=on go install -race -tags='dev debug profile netgo' -ldflags='$(ldflags)' $(pkgs)

# release builds and installs release binaries.
release:
	GO111MODULE=on go install -tags='netgo' -ldflags='-s -w $(ldflags)' $(pkgs)
release-race:
	GO111MODULE=on go install -race -tags='netgo' -ldflags='-s -w $(ldflags)' $(pkgs)

# clean removes all directories that get automatically created during
# development.
clean:
	rm -rf cover doc/whitepaper.aux doc/whitepaper.log doc/whitepaper.pdf fullcover release 

test:
	GO111MODULE=on go test -short -tags='debug testing netgo' -timeout=5s $(pkgs) -run=$(run)
test-utils:
	GO111MODULE=on go test -short -tags='debug testing netgo' -timeout=5s $(utils) -run=$(run)
test-v:
	GO111MODULE=on go test -race -v -short -tags='debug testing netgo' -timeout=15s $(pkgs) -run=$(run)
test-long: clean fmt vet lint
	@mkdir -p cover
	GO111MODULE=on go test --coverprofile='./cover/cover.out' -v -race -failfast -tags='testing debug netgo' -timeout=1800s $(pkgs) -run=$(run)
	GO111MODULE=on go test --coverprofile='./cover/cover.out' -v -race -failfast -tags='testing debug netgo' -timeout=180s $(utils) -run=$(run)
test-vlong: clean fmt vet lint
	@mkdir -p cover
	GO111MODULE=on go test --coverprofile='./cover/cover.out' -v -race -failfast -tags='testing debug netgo' -timeout=180s $(utils) -run=$(run)
	GO111MODULE=on go test --coverprofile='./cover/cover.out' -v -race -tags='testing debug vlong netgo' -timeout=20000s $(pkgs) -run=$(run)
test-cpu:
	GO111MODULE=on go test -v -tags='testing debug netgo' -timeout=500s -cpuprofile cpu.prof $(pkgs) -run=$(run)
test-mem:
	GO111MODULE=on go test -v -tags='testing debug netgo' -timeout=500s -memprofile mem.prof $(pkgs) -run=$(run)
bench: clean fmt
	GO111MODULE=on go test -tags='debug testing netgo' -timeout=500s -run=XXX -bench=$(run) $(pkgs)
cover: clean
	@mkdir -p cover
	@for package in $(pkgs); do                                                                                                                                 \
		mkdir -p `dirname cover/$$package`                                                                                                                      \
		&& GO111MODULE=on go test -tags='testing debug netgo' -timeout=500s -covermode=atomic -coverprofile=cover/$$package.out ./$$package -run=$(run) || true \
		&& go tool cover -html=cover/$$package.out -o=cover/$$package.html ;                                                                                    \
	done

# fullcover is a command that will give the full coverage statistics for a
# package. Unlike the 'cover' command, full cover will include the testing
# coverage that is provided by all tests in all packages on the target package.
# Only one package can be targeted at a time. Use 'cpkg' as the variable for the
# target package, 'pkgs' as the variable for the packages running the tests.
#
# NOTE: this command has to run the full test suite to get output for a single
# package. Ideally we could get the output for all packages when running the
# full test suite.
#
# NOTE: This command will not skip testing packages that do not run code in the
# target package at all. For example, none of the tests in the 'sync' package
# will provide any coverage to the renter package. The command will not detect
# this and will run all of the sync package tests anyway.
fullcover: clean
	@mkdir -p fullcover
	@mkdir -p fullcover/tests
	@echo "mode: atomic" >> fullcover/fullcover.out
	@for package in $(pkgs); do                                                                                                                                                             \
		mkdir -p `dirname fullcover/tests/$$package`                                                                                                                                        \
		&& GO111MODULE=on go test -tags='testing debug netgo' -timeout=500s -covermode=atomic -coverprofile=fullcover/tests/$$package.out -coverpkg $(cpkg) ./$$package -run=$(run) || true \
		&& go tool cover -html=fullcover/tests/$$package.out -o=fullcover/tests/$$package.html                                                                                              \
		&& tail -n +2 fullcover/tests/$$package.out >> fullcover/fullcover.out ;                                                                                                            \
	done
	@go tool cover -html=fullcover/fullcover.out -o fullcover/fullcover.html
	@printf 'Full coverage on $(cpkg):'
	@go tool cover -func fullcover/fullcover.out | tail -n -1 | awk '{$$1=""; $$2=""; sub(" ", " "); print}'

utils:
	GO111MODULE=on go install -tags='netgo' -ldflags='$(ldflags)' $(utils)

# whitepaper builds the whitepaper from whitepaper.tex. pdflatex has to be
# called twice because references will not update correctly the first time.
whitepaper:
	@pdflatex -output-directory=doc whitepaper.tex > /dev/null
	pdflatex -output-directory=doc whitepaper.tex

.PHONY: all fmt install release clean test test-v test-long cover whitepaper

