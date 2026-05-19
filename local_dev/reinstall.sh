# Source this script to reinstall packages from local repo e.g.
#   $ local_dev/reinstall.sh
pip uninstall -y globus-compute-sdk globus-compute-endpoint

SDK="$(dirname $0)/../compute_sdk"
EP="$(dirname $0)/../compute_endpoint"
pip install -e $SDK -e $EP
