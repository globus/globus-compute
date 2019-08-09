#!/bin/bash

VERSION=$1

FUNCX_VERSION=$(python3 -c "import funcx; print(funcx.__version__)")

if [[ $FUNCX_VERSION == $VERSION ]]
then
    echo "Version requested matches package version: $VERSION"
else
    echo "[ERROR] Version mismatch. User request:$VERSION while package version is:$FUNCX_VERSION"
    exit -1
fi


create_tag () {

    echo "Creating tag"
    git tag -a "$VERSION" -m "Funcx $VERSION"

    echo "Pushing tag"
    git push origin --tags

}


release () {
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
    twine upload dist/*
}


create_tag
release

