#!/usr/bin/env bash
mkdir ~/.funcx
mkdir ~/.funcx/$1
mkdir ~/.funcx/credentials
cp /compute/config/config.py ~/.funcx
cp /compute/$1/* ~/.funcx/$1
cp /compute/credentials/storage.db ~/.funcx/
if [ -z "$2" ]; then
  globus-compute-endpoint start $1
else
  globus-compute-endpoint start $1 --endpoint-uuid $2
fi

while pgrep globus-compute-endpoint >/dev/null;
    do
        echo "globus-compute-endpoint process is still alive. Next check in 600s."
        sleep 600;
    done
echo "globus-compute-endpoint process exited. Restarting endpoint"
