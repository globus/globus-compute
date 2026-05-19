#!/bin/bash
# Run this script to reinstall packages from local repo e.g.
#   $ local_dev/reinstall.sh # or source local_dev/reinstall.sh

# The following is a bit overkill but is slightly more convenient to use
if [ -n "${BASH_VERSION:-}" ]; then
  SELF="${BASH_SOURCE[0]}"
elif [ -n "${ZSH_VERSION:-}" ]; then
  SELF="${(%):-%x}"
else
  echo Current shell is not supported, please run with ./reinstall.sh
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$SELF")/.." &> /dev/null && pwd)"

SDK="$SCRIPT_DIR/compute_sdk"
EP="$SCRIPT_DIR/compute_endpoint"

pip uninstall -y globus-compute-sdk globus-compute-endpoint

pip install -e "$SDK" -e "$EP"
