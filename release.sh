#!/bin/bash
#
# Script to release a new version of the SDK and the Endpoint
#
# It does this by creating a tag named after the version, then running the
# tox release command for each package
#
# Requirements:
#   the version is set in funcx_sdk
#   the version is set in funcx_endpoint and matches funcx_sdk
#   the version number must appear to be in use in the changelog
#   you must have valid git config to create a signed tag (GPG key)
#   you must have pypi credentials available to twine (e.g. ~/.pypirc)

set -euo pipefail

VERSION="$(grep '^__version__' funcx_sdk/funcx/version.py | cut -d '"' -f 2)"
ENDPOINT_VERSION="$(grep '^__version__' funcx_endpoint/funcx_endpoint/version.py | cut -d '"' -f 2)"

if [[ "$VERSION" != "$ENDPOINT_VERSION" ]]; then
  echo "package versions mismatched: sdk=$VERSION endpoint=$ENDPOINT_VERSION"
  exit 1
fi

if ! grep '^funcx \& funcx\-endpoint v'"$VERSION"'$' docs/changelog.rst; then
  echo "package version v$VERSION not noted in docs/changelog.rst"
  exit 1
fi

echo "releasing v$VERSION"
git tag -s "$VERSION" -m "v$VERSION"

pushd funcx_sdk
tox -e publish-release
popd

cd funcx_endpoint
tox -e publish-release
