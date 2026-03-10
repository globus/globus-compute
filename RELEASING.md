# Releasing

Releases of `globus-compute-sdk` and `globus-compute-endpoint` are always published
in unison and with the same version number, even when only one package has changes.

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
- Globus VPN access

⚠️ The Jenkins build pages need to be accessed via VPN.

## Alpha releases

1. Branch off from the `main` branch to create a release branch (e.g., `v.1.1.0`):

   ```console
   $ git checkout -b v1.1.0 main
   ```

1. Bump the version of both packages, and the endpoint's SDK dependency, to a new alpha release (e.g., `1.0.0` -> `1.1.0a0`):

   ```console
   $ $EDITOR compute_sdk/globus_compute_sdk/version.py compute_endpoint/setup.py compute_endpoint/globus_compute_endpoint/version.py
   ```

1. Commit the changes:

   ```console
   $ git add compute_sdk/globus_compute_sdk/version.py compute_endpoint/setup.py compute_endpoint/globus_compute_endpoint/version.py
   $ git commit -m "Bump versions for alpha release v1.1.0a0"
   $git push -u origin v1.1.0
   ```

1. Run `./release.sh` from the repo root. This script creates a signed tag named after
   the current version and pushes it to GitHub, then uses the `tox` release command
   to push each package to PyPi.

1. Navigate to the [Build with Parameters](https://builds.globus.org/jenkins/job/BuildGlobusComputeAgentPackages/build?delay=0sec) Jenkins page.

1. Enter the name of the release branch (eg, `v4.8.0`) into the `BRANCH_OR_TAG` field. (Leave `BUILD_FOR_STABLE` unchecked.)

1. Click the Build button.

1. Wait 15-30 minutes and confirm that the [build is green](https://builds.globus.org/jenkins/job/BuildGlobusComputeAgentPackages/).

### Alpha release bugfixes

1. Branch off from the release branch to create a new bugfix branch:

   ```console
   $ git checkout -b some-bugfix v1.1.0
   ```

1. Commit your changes and push to GitHub.

   ```console
   $ git add .
   $ git commit -m "Fixed X"
   $ git push -u origin some-bugfix
   ```

1. Open a PR in GitHub to merge the bugfix branch into the release branch.

1. Once the PR is approved and merged, pull the new commits from the remote
   release branch:

   ```console
   $ git checkout v1.1.0
   $ git pull
   ```

1. Repeat steps 2 through 4 of the main [Alpha releases](#alpha-releases) procedure.
   Be sure to only bump the alpha version number (e.g., `1.1.0a0` -> `1.1.0a1`).

## Production releases

1. Checkout the release branch.

   ```console
   $ git checkout v1.1.0
   ```

1. Remove the alpha version designation for both packages and the endpoint's SDK dependency (e.g., `1.1.0a1` -> `1.1.0`):

   ```console
   $ $EDITOR compute_sdk/globus_compute_sdk/version.py compute_endpoint/setup.py compute_endpoint/globus_compute_endpoint/version.py
   ```

1. Update the changelog:

   ```console
   $ scriv collect --edit
   ```

1. Commit the changes:

   ```console
   $ git add changelog.d/ docs/changelog.rst
   $ git add compute_sdk/globus_compute_sdk/version.py compute_endpoint/setup.py compute_endpoint/globus_compute_endpoint/version.py
   $ git commit -m "Bump versions and changelog for release v1.1.0"
   $ git push
   ```

1. Run `./release.sh` from the repo root.

1. Open a PR in GitHub to merge the release branch into `main`.

   **⚠️ Important:** Once approved, merge the PR using the "Merge commit" option.
   This will ensure that the tagged commits and bug fixes from the release branch
   are properly added to the `main` branch.

1. Create a GitHub release from the tag. See [GitHub documentation](https://docs.github.com/en/repositories/releasing-projects-on-github/managing-releases-in-a-repository#creating-a-release)
   for instructions.

1. Navigate to the [Build with Parameters](https://builds.globus.org/jenkins/job/BuildGlobusComputeAgentPackages/build?delay=0sec) Jenkins page.

1. Enter the name of the release branch (eg, `v4.8.0`) into the `BRANCH_OR_TAG` field, and ensure `BUILD_FOR_STABLE` is checked.

1. Click the Build button.

1. Wait 15-30 minutes and confirm that the [build is green](https://builds.globus.org/jenkins/job/BuildGlobusComputeAgentPackages/).

1. Depending on whether GCS is also releasing:
   - If GCS deploys after Compute on release day, the new packages will be pushed to the public repos as part of their deploy, so no action is needed.
   - If GCS is not doing a release the same week, or if they finish their deploy before we finish building our packages, we need to manually run the downloads sync Jenkins script:
     - https://builds.globus.org/jenkins/view/all/job/Synchronize%20GCSv5%20Stable/build?delay=0sec
       - Leave `SYNC_WHEELS_ONLY` unchecked

## DEB/RPM Packaging Notes

Our alpha builds will go to the `unstable` repo, and production packages goes to both
the `testing` and `stable` repos.

After this build process for production, the testing and stable packages will reside
in an internal globus 'holding' repo. GCS manages the infrastructure so we need to
run another Jenkins build to push it to live if GCS is not doing a release the same week which also pushes our packages. See last pipeline step above.

- Example of unstable repo:
  - https://downloads.globus.org/globus-connect-server/unstable/rpm/el/9/x86_64/
- Directory of testing images:
  - https://builds.globus.org/downloads.globus.org/globus-connect-server/testing/rpm/el/8/x86_64/
- Stable repo:
  - The images will be in the below build directory after we finish our build process, but not public:
    - https://builds.globus.org/downloads.globus.org/globus-connect-server/stable/rpm/el/8/x86_64/
  - After GCS push during deploy day (or if we ping them to do so), the public images will be located at:
    - https://downloads.globus.org/globus-connect-server/stable/rpm/el/9/x86_64/
      [publishResults.groovy line 85](https://github.com/globusonline/gcs-build-scripts/blob/168617a0ccbb0aee7b3bee04ee67940bbe2a80f6/vars/publishResults.groovy#L85)
