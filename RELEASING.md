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

## Alpha releases

1. Branch off from the `main` branch to create a release branch (e.g., `v.1.1.0`):

```bash
git checkout -b v1.1.0 main
```

2. Bump the version of both packages to a new alpha release (e.g., `1.0.0` -> `1.1.0a0`):

```bash
$EDITOR compute_sdk/globus_compute_sdk/version.py compute_endpoint/setup.py compute_endpoint/globus_compute_endpoint/version.py
```

3. Commit the changes:

```bash
git add compute_sdk/globus_compute_sdk/version.py compute_endpoint/setup.py compute_endpoint/globus_compute_endpoint/version.py
git commit -m "Bump versions for alpha release v1.1.0a0"
git push -u origin v1.1.0
```

4. Run `./release.sh` from the repo root. This script creates a signed tag named after
   the current version and pushes it to GitHub, then uses the `tox` release command
   to push each package to PyPi.

### Alpha release bugfixes

1. Branch off from the release branch to create a new bugfix branch:

```bash
git checkout -b some-bugfix v1.1.0
```

2. Commit your changes and push to GitHub.

```bash
git add .
git commit -m "Fixed X"
git push -u origin some-bugfix
```

3. Open a PR in GitHub to merge the bugfix branch into the release branch.

4. Once the PR is approved and merged, pull the new commits from the remote
   release branch:

```bash
git checkout v1.1.0
git pull
```

5. Repeat steps 2 through 4 of the main [Alpha releases](#alpha-releases) procedure.
   Be sure to only bump the alpha version number (e.g., `1.1.0a0` -> `1.1.0a1`).

## Production releases

1. Checkout the release branch.

```bash
git checkout v1.1.0
```

2. Remove the alpha version designation for both packages (e.g., `1.1.0a1` -> `1.1.0`):

```bash
$EDITOR compute_sdk/globus_compute_sdk/version.py compute_endpoint/setup.py compute_endpoint/globus_compute_endpoint/version.py
```

2. Update the changelog:

```bash
scriv collect --edit
```

3. Commit the changes:

```bash
git add changelog.d/ docs/changelog.rst
git add compute_sdk/globus_compute_sdk/version.py compute_endpoint/setup.py compute_endpoint/globus_compute_endpoint/version.py
git commit -m "Bump versions and changelog for release v1.1.0"
git push
```

5. Run `./release.sh` from the repo root.

5. Open a PR in GitHub to merge the release branch into `main`.

   **⚠️ Important:** Once approved, merge the PR using the "Merge commit" option.
   This will ensure that the tagged commits and bug fixes from the release branch
   are properly added to the `main` branch.

6. Create a GitHub release from the tag. See [GitHub documentation](https://docs.github.com/en/repositories/releasing-projects-on-github/managing-releases-in-a-repository#creating-a-release)
   for instructions.