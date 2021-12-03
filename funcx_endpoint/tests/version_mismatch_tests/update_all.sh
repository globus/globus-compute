#!/bin/bash

VERSIONS=("3.6" "3.7" "3.8" "3.9")
SRC_PATH=$1

conda init bash
for py_v in ${VERSIONS[*]}
do
    env_name="funcx_version_mismatch_py$py_v"
    echo "Updating $env_name"
    conda deactivate
    conda activate $env_name
    echo $CONDA_PREFIX
    pushd .
    cd $SRC_PATH
    pip install ./funcx_sdk ./funcx_endpoint
    pip install parsl
    popd
done

