# Releasing

The release of these wrapper packages of globus-compute-sdk and globus-compute-endpoint are intended
to support backwards compatibility for users who do not want to utilize the new globus-compute* packages.

This release process is partially automated with tools to help along the way.

## Prerequisites

You must have the following tools installed and available:

- `git`
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

2. Update the changelog by copying the new fragments from the Globus Compute ../docs/changelog.rst
   (Presumably we want to release a new wrapper each time we update Globus Compute SDK/Endpoint)

3. Add and commit the changes to version numbers and changelog files e.g. as follows

```bash
git add changelog.rst sdk/funcx/version.py endpoint/funcx_endpoint/version.py
git commit -m 'Bump versions and changelog for release'
git push
```

4. Run the release script `./release_funcx.sh` from the wrapper root /compute_funcx. This will use
   `tox` and your pypi credentials and will create a signed release tag. At the end of this step,
   new wrapper funcx and funcx-endpoint packages will be published to pypi.

5. Push the release tag, e.g. `git push upstream 2.0.2`
