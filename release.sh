#!/bin/bash
#
# Script to release a new version of the SDK and the Endpoint
#
# It does this by creating a tag named after the version, then running the
# tox release command for each package
#
# Requirements:
#   the version is set in globus_compute_sdk
#   the version is set in globus_compute_endpoint and matches globus_compute_sdk
#   the version number must appear to be in use in the changelog
#   you must have valid git config to create a signed tag (GPG key)
#   you must have pypi credentials available to twine (e.g. ~/.pypirc)

set -euo pipefail

VERSION="$(grep '^__version__' compute_sdk/globus_compute_sdk/version.py | cut -d '"' -f 2)"
ENDPOINT_VERSION="$(grep '^__version__' compute_endpoint/globus_compute_endpoint/version.py | cut -d '"' -f 2)"

if [[ "$VERSION" != "$ENDPOINT_VERSION" ]]; then
  echo "package versions mismatched: sdk=$VERSION endpoint=$ENDPOINT_VERSION"
  exit 1
fi

if ! grep '^globus\-compute\-sdk \& globus\-compute\-endpoint v'"$VERSION"'$' docs/changelog.rst; then
  read -p "Package version v$VERSION not noted in docs/changelog.rst.  Proceed? [y/n] " -n 1 -r
  if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    [[ $0 = $BASH_SOURCE ]] && exit 1 || return 1 # handle exits from shell or function but don't exit interactive shell
  else
    echo "\n  Releasing globus-compute-sdk and globus-compute-endpoint without changelog updates for v$VERSION"
  fi
fi

echo "releasing v$VERSION"
if git tag -s -m "v$VERSION" "$VERSION" ; then
  echo "Git tagged $VERSION"
else
  read -p "Tag $VERSION already exists.  Release packages with this tag anyway? [y/n] " -n 1 -r
  if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    [[ $0 = $BASH_SOURCE ]] && exit 1 || return 1 # handle exits from shell or function but don't exit interactive shell
  fi
fi

pushd compute_sdk
tox -e publish-release
popd

cd compute_endpoint
tox -e publish-release
