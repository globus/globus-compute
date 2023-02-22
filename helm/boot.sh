#!/usr/bin/env bash

EP_NAME="$1"; shift
EP_UUID="$1"; shift

mkdir -p "$HOME/.funcx/$EP_NAME/"
cp /funcx/"$EP_NAME"/* "$HOME/.funcx/$EP_NAME/"
cp /funcx/config/config.py "$HOME/.funcx/"

if [[ -e "/funcx/credentials/storage.db" ]]; then
    cp /funcx/credentials/storage.db "$HOME/.funcx/"
    chmod 600 "$HOME/.funcx/storage.db"
fi

exec funcx-endpoint start "$EP_NAME" --endpoint-uuid "$EP_UUID" "$@"
