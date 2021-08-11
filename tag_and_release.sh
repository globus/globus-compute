#!/bin/bash
# Script to release a new version of the SDK and the Endpoint
# It does this by creating a branch named after the version, updating
# version.py in both packages, committing those changes to the branch and
# then finally building wheels and deploying to pypi
#
# Usage:
# tag_and_release.sh version pypi_username
# The version should be the semantic version for this new release
#
# you will be prompted twice for your pypi password

if [ $# -ne 2 ]; then
    echo "Usage tag_and_release version pypi_username"
    exit 1
fi

VERSION=$1
PYPI_USERNAME=$2

verify_version() {
  pushd $1

  if [[ $1 == 'funcx_sdk' ]]
  then
    PACKAGE="funcx"
    VERSION_FILE_PATH="funcx/sdk/version.py"
  else
    PACKAGE="funcx_endpoint"
    VERSION_FILE_PATH="funcx_endpoint/version.py"
  fi

  FUNCX_VERSION=$(python3 -c "import $PACKAGE; print($PACKAGE.__version__)")

  if [[ $FUNCX_VERSION == $VERSION ]]
  then
      echo "Version requested matches package version: $VERSION"
  else
      echo "Updating version.py to match release"
      sed "s/__version__ *= *\".*\"/__version__ = \"$VERSION\"/" $VERSION_FILE_PATH > $VERSION_FILE_PATH".bak"
      mv  $VERSION_FILE_PATH.bak $VERSION_FILE_PATH
      git status
  fi
  popd
}


create_release_branch () {
    echo "Creating branch"
    git branch -b $VERSION
    git add funcx_endpoint/funcx_endpoint funcx_sdk/funcx
    git commit -m "Update to version $VERSION"

    echo "Pushing branch"
    git push origin $VERSION
}


release () {
    pushd $1
    rm dist/*

    echo "======================================================================="
    echo "Starting clean builds"
    echo "======================================================================="
    python3 setup.py sdist
    python3 setup.py bdist_wheel

    echo "======================================================================="
    echo "Done with builds"
    echo "======================================================================="
    sleep 1
    echo "======================================================================="
    echo "Push to PyPi. This will require your username and password"
    echo "======================================================================="
    twine upload -u $PYPI_USERNAME dist/*
    popd
}

verify_version funcx_sdk
verify_version funcx_endpoint
create_release_branch
release funcx_sdk
release funcx_endpoint

