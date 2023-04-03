#!/bin/bash

# Run shell scripts found in this directory and subdirectories.
# N.B. this script searches for *executable* files only.

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"
cd "$script_dir"

find . -type f -name "test_*.sh" -executable -exec /bin/echo -n "({}) " \; -exec {} \;
