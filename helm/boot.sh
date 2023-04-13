#!/usr/bin/env bash

set -e

EP_NAME="$1"; shift
EP_UUID="$1"; shift

echo -e "\n  Preparing to start kubelet Endpoint: $EP_UUID ($EP_NAME)\n"

mkdir -p "$HOME/.globus_compute/$EP_NAME/"
cp /compute/ep_instance/* "$HOME/.globus_compute/$EP_NAME/"
cp /compute/config/config.py "$HOME/.globus_compute/"

if [[ -e "/compute/credentials/storage.db" ]]; then
    cp /compute/credentials/storage.db "$HOME/.globus_compute/"
    chmod 600 "$HOME/.globus_compute/storage.db"
fi

exec globus-compute-endpoint start "$EP_NAME" --endpoint-uuid "$EP_UUID" "$@"
