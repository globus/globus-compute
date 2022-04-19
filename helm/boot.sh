#!/usr/bin/env bash
mkdir ~/.funcx
mkdir ~/.funcx/$1
mkdir ~/.funcx/credentials
cp /funcx/config/config.py ~/.funcx
cp /funcx/$1/* ~/.funcx/$1
cp /funcx/credentials/storage.db ~/.funcx/
if [ -z "$2" ]; then
  funcx-endpoint start $1
else
  funcx-endpoint start $1 $2
fi

while pgrep funcx-endpoint >/dev/null;
    do
        echo "funcx-endpoint process is still alive. Next check in 600s."
        sleep 600;
    done
echo "funcx-endpoint process exited. Restarting endpoint"
