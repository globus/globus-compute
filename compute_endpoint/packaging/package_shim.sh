#!/bin/sh

VENV_DIR="@VIRTUAL_ENV@"

if type deactivate 1> /dev/null 2> /dev/null; then
    deactivate
fi

. "$VENV_DIR"/bin/activate

exec "$VENV_DIR"/bin/globus-compute-endpoint "$@"