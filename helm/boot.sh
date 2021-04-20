#!/usr/bin/env bash
mkdir ~/.funcx
mkdir ~/.funcx/$1
mkdir ~/.funcx/credentials
cp /funcx/config/config.py ~/.funcx
cp /funcx/$1/* ~/.funcx/$1
cp /funcx/credentials/* ~/.funcx/credentials
if [ -z "$2" ]; then
  funcx-endpoint start $1
else
  funcx-endpoint start $1 --endpoint-uuid $2
fi

sleep infinity
