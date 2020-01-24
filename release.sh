#!/usr/bin/env bash
set -e

# version and keys are supplied as arguments
version="$1"
rc=`echo $version | awk -F - '{print $2}'`
keyfile="$2"
pubkeyfile="$3" # optional
if [[ -z $version ]]; then
	echo "Usage: $0 VERSION "
	exit 1
fi

# setup build-time vars
ldflags="-s -w -X 'gitlab.com/NebulousLabs/Sia/build.GitRevision=`git rev-parse --short HEAD`' -X 'gitlab.com/NebulousLabs/Sia/build.BuildTime=`date`' -X 'gitlab.com/NebulousLabs/Sia/build.ReleaseTag=${rc}'"

function build {
  os=$1
  arch=$2

	echo Building ${os}...
	# create workspace
	folder=release/Sia-$version-$os-$arch
	rm -rf $folder
	mkdir -p $folder
	# compile and hash binaries
	for pkg in siac siad; do
		bin=$pkg
		if [ "$os" == "windows" ]; then
			bin=${pkg}.exe
		fi
		CGO_ENABLED=0 GOOS=${os} GOARCH=${arch} go build -a -tags 'netgo' -trimpath -ldflags="$ldflags" -o $folder/$bin ./cmd/$pkg
    sha256sum $folder/$bin >> release/Sia-$version-SHA256SUMS.txt
  done
}

function package {
  os=$1
  arch=$2

	echo Packaging ${os}...
	folder=release/Sia-$version-$os-$arch

	# add other artifacts and file of hashes.
	cp -r release/Sia-$version-SHA256SUMS.txt doc LICENSE README.md $folder
	# zip
	(
		cd release
		zip -rq Sia-$version-$os-$arch.zip Sia-$version-$os-$arch
	)
}

build "linux" "amd64"
package "linux" "amd64"
