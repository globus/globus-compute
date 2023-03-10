#!/usr/bin/env bash

EP_NAME="$1"; shift
EP_UUID="$1"; shift

echo -e "\n  Preparing to start kubelet Endpoint: $EP_UUID ($EP_NAME)\n"

mkdir -p "$HOME/.funcx/$EP_NAME/"
cp /compute/"$EP_NAME"/* "$HOME/.funcx/$EP_NAME/"
cp /compute/config/config.py "$HOME/.funcx/"

if [[ -e "/compute/credentials/storage.db" ]]; then
    cp /compute/credentials/storage.db "$HOME/.funcx/"
    chmod 600 "$HOME/.funcx/storage.db"
fi

exec globus-compute-endpoint start "$EP_NAME" --endpoint-uuid "$EP_UUID" "$@"
