# Releasing

The release of these wrapper packages of globus-compute-sdk and globus-compute-endpoint are intended
to support backwards compatibility for users who do not want to utilize the new globus-compute* packages.

This release process is partially automated with tools to help along the way.

## Prerequisites

You must have the following tools installed and available:

- `git`
- `scriv`
- `tox`

You will also need the following credentials:

- a configured GPG key in `git` in order to create signed tags
- pypi credentials for use with `twine` (e.g. a token in `~/.pypirc`) valid for
    publishing `funcx` and `funcx-endpoint`

## Procedure

1. Bump versions of both packages to a new latest version number by removing
   the alpha `a0` suffix, if any, and using the next higher number.

```bash
$EDITOR sdk/funcx/version.py endpoint/funcx_endpoint/version.py
```

2. Update the changelog by running `scriv collect --edit`

3. Add and commit the changes to version numbers and changelog files (including
   the removal of `changelog.d/` files), e.g. as follows

```bash
git add changelog.d/ docs/changelog.rst
git add sdk/funcx/version.py endpoint/funcx_endpoint/version.py
git commit -m 'Bump versions and changelog for release'
git push
```

4. Run the release script `./release.sh` from the repo root. This will use
   `tox` and your pypi credentials and will create a signed release tag. At the
   end of this step, new packages will be published to pypi.

5. Push the release tag, e.g. `git push upstream 2.0.2`

6. Update the version numbers to the next point version and re-add the `a0` suffix,
   if necessary, then commit and push, e.g.

```bash
$EDITOR sdk/funcx/version.py endpoint/funcx_endpoint/version.py
git add sdk/funcx/version.py endpoint/funcx_endpoint/version.py
git commit -m 'Bump versions for release'
git push
```
