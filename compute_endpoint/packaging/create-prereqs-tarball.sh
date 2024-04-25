#!/bin/bash

set -e
serr() { >&2 echo "$@"; }

usage() {
  serr "usage: $0 <path_to_python_source_project_with_setup_py> > <some_name.txz>"
  serr
  serr "Writes to stdout a tarball (XZ compressed) of the specified Python package's"
  serr "prerequisite wheels."
  serr -e "\n  Examples:\n"
  serr "    $ $0 some/python/project/ > my_project.txz"
  serr "    $ $0 some/python/project/ | some_other_filter [| ...]"

  if [[ -n "$1" ]]; then
    serr -e "\n\033[91;1;40m$1\033[39;49m"
  fi
  exit 1
}

[[ -e / ]] || { serr "Invalid script interpreter (expecting Bash); exiting"; exit 1; }

[[ -z $1 ]] && usage "Missing a path to a project source"

[[ -t 1 ]] && usage "Cowardly refusing to write archive contents to the terminal"


PYTHON_BIN="${PYTHON_BIN:-/opt/globus/bin/python3}"
[[ -x $PYTHON_BIN ]] || { serr "'$PYTHON_BIN' not found or not executable"; exit 2; }

DEBUG="${DEBUG:-}"
TMPDIR_PREFIX=".tmp.prereqs."

if ! command -v realpath > /dev/null; then
  realpath() { (cd "$1" && pwd); }
fi

_ORIG_DIR="$(realpath "$(pwd)")"

_cleanup() {
  cd "$_ORIG_DIR"
  if [[ -n $DEBUG ]]; then
    serr "$0 -- DEBUG requested; not cleaning up:"
    serr "  - $prereqs_dir"
    return
  fi

  if [[ -d $prereqs_dir && $prereqs_dir == *"$TMPDIR_PREFIX"* ]]; then
    rm -rf "$prereqs_dir"
  else
    serr -e "\n  Notice - did not cleanup temporary directory: '$prereqs_dir'"
  fi
}
trap _cleanup EXIT

src_dir="$(realpath "$1/")"
shift 1

# RPM and DEB packages don't understand -prerelease bits from semver spec, so
# replace with ~ that works almost the same
py_full_version=$("$PYTHON_BIN" -c "import sys; print('{}.{}.{}'.format(*sys.version_info))")
[[ -z $py_version ]] && py_version="$(echo "$py_full_version" | cut -d . -f1,2 | tr -d '.')"
[[ -z $pkg_version ]] && pkg_version="$(cd "$src_dir"; "$PYTHON_BIN" setup.py --version | tr '-' '~')"
[[ -z $pkg_name ]] && pkg_name="$(cd "$src_dir"; "$PYTHON_BIN" setup.py --name | tr '-' '_')"

prereqs_dir="$(mktemp -q -d -p "./" "${TMPDIR_PREFIX}XXXXXXXXXX")"
download_dir="$pkg_name-prereqs-py$py_version-$pkg_version"

cd "$prereqs_dir"
mkdir "$download_dir/"

"$PYTHON_BIN" -m venv venv.download-prereqs
. ./venv.download-prereqs/bin/activate

>&2 python -m pip install -U pip
>&2 python -m pip install wheel setuptools
>&2 python -m pip download -d "$download_dir" "$src_dir" pip
>&2 python -m pip download -d "$download_dir" "$src_dir"
>&2 python -m pip download -d "$download_dir" --python-version $py_version --platform manylinux2014_x86_64 --no-deps "$src_dir"

for p in "$download_dir/"*.whl; do
    name=$(basename "$p" .whl)
    wname="${name%-*-*-*}"
    wver="${wname##*-}"
    wname="${wname%-$wver}"
    echo "$wname"
done > dependent-prereqs.txt

>&2 python -m pip download -d "$download_dir" --python-version $py_version --platform manylinux2014_x86_64 --no-deps -r "dependent-prereqs.txt" setuptools

modified_time="$(TZ=UTC0 date --rfc-3339=seconds)"

TZ=UTC0 tar --format=posix \
  --pax-option=exthdr.name=%d/PaxHeaders/%f,delete=atime,delete=ctime,delete=mtime \
  --mtime="$modified_time" \
  --numeric-owner \
  --owner=0 \
  --group=0 \
  --mode="go-rwx,u+rw" \
  -cf - "$download_dir/" \
  | gzip -9
