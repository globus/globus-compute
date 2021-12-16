#!/bin/bash

fn

pwd=$1
cd $pwd

VERSIONS=("3.6" "3.7" "3.8" "3.9")
conda init bash

# This will fail if the endpoint configuration already exists
# funcx-endpoint configure mismatched

# Copy over configs
cp config.py ~/.funcx/mismatched


start_endpoint() {
    worker_v=$1
    ep_v=$2
    export WORKER_CONDA_ENV=$w_env_name
    export WORKER_CONDA_ENV=$w_env_name
    echo "Running ep with $WORKER_CONDA_ENV with python $worker_v"
    funcx-endpoint start mismatched
    endpoint_id=$(funcx-endpoint list | grep mismatched | awk '{print $6}')
    sleep 2
    python3 $pwd/test_mismatched.py -e $endpoint_id -w $worker_v -v $ep_v
    if [ $? -eq 0 ]; then
        echo "TEST PASSED, EP_PY_V:$ep_v WORKER_PY_V:$worker_v"
    else
        echo "TEST FAILED, EP_PY_V:$ep_v WORKER_PY_V:$worker_v"
        return 1
    fi
    echo "Stopping endpoint in 2s"
    sleep 2
    funcx-endpoint stop mismatched
    sleep 2
}

run_test () {
    funcx_endpoint list mismatched
    python3 test_mismatched.py $worker_v
}


for ep_v in ${VERSIONS[*]}
# for ep_v in "3.7"
do
    for worker_v in ${VERSIONS[*]}
    # for worker_v in "3.9"
    do
        ep_env_name="funcx_version_mismatch_py$ep_v"
        w_env_name="funcx_version_mismatch_py$worker_v"
        echo "Testing EP:python=$ep_v against Worker:python=$worker_v"
        conda activate $ep_env_name
        which funcx-endpoint
        start_endpoint $worker_v $ep_v
        if [ $? -ne 0 ] ; then
          echo Aborting tests due to failure.
          exit 1
        fi
    done
done

