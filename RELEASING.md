# Releasing

Releases of `globus-compute-sdk` and `globus-compute-endpoint` are always done with a single version
number, even when only one package has changes.

The process is partially automated with tools to help along the way.

## Prerequisites

You must have the following tools installed and available:

- `git`
- `scriv`
- `tox`

You will also need the following credentials:

- a configured GPG key in `git` in order to create signed tags
- pypi credentials for use with `twine` (e.g. a token in `~/.pypirc`) valid for
    publishing `globus-compute-sdk` and `globus-compute-endpoint`

## Procedure

1. Bump versions of both packages to a new latest version number.  This is
   the next higher alpha version number like 2.16.0a1 if 2.16.0a0 already
   exists, or <X>.<Y+1>.<Z>a0 if this will be the first alpha release for an
   upcoming production release when the existing PyPI version is <X>.<Y>.<Z>

```bash
$EDITOR compute_sdk/globus_compute_sdk/version.py compute_endpoint/setup.py compute_endpoint/globus_compute_endpoint/version.py
```

2. Update the changelog by running `scriv collect --edit`

3. Add and commit the changes to version numbers and changelog files (including
   the removal of `changelog.d/` files), e.g. as follows

```bash
git add changelog.d/ docs/changelog.rst
git add compute_sdk/globus_compute_sdk/version.py compute_endpoint/globus_compute_endpoint/version.py
git commit -m 'Bump versions and changelog for release'
git push
```

4. Run the release script `./release.sh` from the repo root. This will use
   `tox` and your pypi credentials and will create a signed release tag. At the
   end of this step, new packages will be published to pypi.

5. Push the release tag, e.g. `git push upstream 2.0.2`

6. Update the version numbers to the next point version and re-add the `-dev` suffix,
   then commit and push, e.g.

```bash
$EDITOR compute_sdk/globus_compute_sdk/version.py compute_endpoint/globus_compute_endpoint/version.py
git add compute_sdk/globus_compute_sdk/version.py compute_endpoint/globus_compute_endpoint/version.py
git commit -m 'Bump versions for dev'
git push
```

