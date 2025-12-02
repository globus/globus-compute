#!/usr/bin/env bash

# Take advantage of FS-events to avoid an up-enter cycle.  Just save any file within
# the documentation directory and then go reload the page.
#
# N.B.: difference must be visible via `git diff`; new files won't get picked up unless
# at least added to the git index.

PORT="${1:-12345}"

renice -n +1000 -p $$ &> /dev/null
command -v ionice &> /dev/null && ionice -c 2 -n 7 -p $$

if command -v inotifywait &> /dev/null; then
    WATCH_FS_COMMAND="inotifywait -re modify"
elif command -v fswatch &> /dev/null; then
    WATCH_FS_COMMAND="fswatch -1"
else
    echo "No known command to watch filesystem; bailing so as not waste CPU cycles"
    exit 2
fi

doc_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"
cd "$doc_dir" || { echo "Unable to change directory to '$doc_dir';"; exit 2; }

dev_venvs=(  # add dev-specific venvs activate paths here
    ../venvs/gcdocs/bin/activate
    ../.venv-docs/bin/activate  # created by ../Makefile (`make docs`)
)
for venv_activate in "${dev_venvs[@]}"; do
    [[ -f "$venv_activate" ]] && {
        source "$venv_activate" || exit 2
        _ACTIVATED=1
        break
    }
done

if [[ -z $_ACTIVATED ]]; then
    (cd ..; make .venv-docs) || exit 2
    source ../.venv-docs/bin/activate || exit 2
else
    python -m pip install -U pip setuptools || exit 2
    python -m pip install -e '../compute_sdk[docs]' -e '../compute_endpoint' || exit 2
fi

# inaugural run
make clean html || exit 2

echo -en "\n\033[40;92;1m" # highlight python 'http.server' message
(cd _build/html/; python3 -m http.server -b localhost $PORT) &
sleep 1
echo -en "\033[m"

P="$(git diff)"  # "previous"
while : ; do
    echo -en "\033[G\033[K$(date) Settling ..."
    sleep 3  # Just give some settling time
    echo -en "\033[G\033[K$(date) Waiting for changes ..."
    $WATCH_FS_COMMAND ./ &> /dev/null
    N="$(git diff)"  # "now"
    [[ $N = "$P" ]] && continue  # don't waste cycles if no worthy change
    echo -en "\033[G\033[K"
    P="$N"
    make html
    echo -e "\n"
done
