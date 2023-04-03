#!/bin/bash

set -e

env="smoke_test_environment"

conf_dir="$(eval echo ~"/.funcx/smoke_test")"

cleanup(){
    [[ -d $conf_dir ]] && rm -rf "$conf_dir"
    pkill -P $$
}

(sleep 10s; echo >&2 "Test took too long; killing."; cleanup; pkill -9 -g 0) &

trap cleanup EXIT

echo -n "New install initiates native login flow: "

funcx-endpoint configure smoke_test > /dev/null # 2>&1

if [[ ! -d "$conf_dir/" ]]; then
    echo "Failed to make directory: $conf_dir/"
    exit 2
fi

cat > "$conf_dir/config.py" <<EOF
from parsl.providers import LocalProvider

from funcx_endpoint.endpoint.utils.config import Config
from funcx_endpoint.executors import HighThroughputExecutor

config = Config(
    executors=[
        HighThroughputExecutor(
            provider=LocalProvider(init_blocks=1, min_blocks=0, max_blocks=1),
            heartbeat_period=10,
        )
    ],
    environment="$env",
    detach_endpoint=False,
)
EOF

exit_code=3
while read -r line; do
    if [[ "Please authenticate with Globus here" = *"$line"* ]]; then
       exit_code=0
       break
    fi
done < <(exec timeout -k 1 5 funcx-endpoint start smoke_test 2>&1 || true)

[[ $exit_code -eq 0 ]] && msg="32mPASSED" || msg="31mFAILED"
echo -e "\033[$msg\033[0m"
exit $exit_code
