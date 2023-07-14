#!/bin/bash
#
# Script to release a new version of the SDK and the Endpoint
#
# It does this by creating a tag named after the version, then running the
# tox release command for each package
#
# Requirements:
#   the version is set in sdk
#   the version is set in endpoint and matches compute_funcx/sdk
#   you must have valid git config to create a signed tag (GPG key)
#   you must have pypi credentials available to twine (e.g. ~/.pypirc)

set -euo pipefail

VERSION="$(grep '^__version__' sdk/funcx/version.py | cut -d '"' -f 2)"
ENDPOINT_VERSION="$(grep '^__version__' endpoint/funcx_endpoint/version.py | cut -d '"' -f 2)"

if [[ "$VERSION" != "$ENDPOINT_VERSION" ]]; then
  echo "package versions mismatched: sdk=$VERSION endpoint=$ENDPOINT_VERSION"
  exit 1
fi

if ! grep '^funcx \& funcx\-endpoint v'"$VERSION"'$' ../docs/changelog_funcx.rst; then
  read -p "Package version v$VERSION not noted in docs/changelog_funcx.rst.  Proceed? [y/n] " -n 1 -r
  if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    [[ $0 = $BASH_SOURCE ]] && exit 1 || return 1 # handle exits from shell or function but don't exit interactive shell
  else
    echo "\n  Releasing funcx wrappers without changelog updates for v$VERSION"
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

pushd sdk
tox -e publish-release
popd

pushd endpoint
tox -e publish-release
popd
