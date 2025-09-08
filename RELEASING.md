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

### DEB/RPM Packaging Workflow

#### Pre-requisites

Before building the packages:

* ensure that the release itself, either the alpha or prod versions, is published on PyPI.
* ⚠️ The Jenkins build pages need to be accessed via VPN.

#### Build Process

To build the DEB/RPM packages after the alpha/prod PyPI is released, specify the alpha or prod
tag names as detailed below and then click the green **Build** button.

##### Notes
    Our alpha builds will go to the ``unstable`` repo, and production packages goes
     to both the ``testing`` and ``stable`` repos.

    After this build process for production, the testing and stable packages will
    reside in an internal globus 'holding' repo.  GCS manages the infrastructure
    so we need to run another Jenkins build to push it to live if GCS is not doing
    a release the same week which also pushes our packages.  See last pipeline step below.

    * Example of unstable repo:
        * https://downloads.globus.org/globus-connect-server/unstable/rpm/el/9/x86_64/
    * Directory of testing images:
        * https://builds.globus.org/downloads.globus.org/globus-connect-server/testing/rpm/el/8/x86_64/
    * Stable repo:
        * The images will be in the below build directory after we finish our build process, but not public:
            * https://builds.globus.org/downloads.globus.org/globus-connect-server/stable/rpm/el/8/x86_64/
        * After GCS push during deploy day (or if we ping them to do so), the public images will be located at:
            * https://downloads.globus.org/globus-connect-server/stable/rpm/el/9/x86_64/
      [publishResults.groovy line 85](https://github.com/globusonline/gcs-build-scripts/blob/168617a0ccbb0aee7b3bee04ee67940bbe2a80f6/vars/publishResults.groovy#L85)
1. (Access on VPN) For each release, update the Pipeline -> SCM -> Branch Specifier to the current release branch name e.g. v3.14.0 in [Build Configuration](https://builds.globus.org/jenkins/job/BuildGlobusComputeAgentPackages/configure)
1. Enter the alpha or prod release name e.g. v3.14.0a0 or v3.14.0 in the input textbox of the [build page here](https://builds.globus.org/jenkins/job/BuildGlobusComputeAgentPackages/build?delay=0sec).  Check the ``BUILD_FOR_STABLE`` box if building for production
1. Wait 20-30 minutes and confirm that the [build is green](https://builds.globus.org/jenkins/job/BuildGlobusComputeAgentPackages/)
1. For production release cycles where there is also a GCS release, if we push our packages before they do, skip the following (also not necessary for alpha releases)
    * If there isn't a concurrent GCS release, or if GCS finishes their deploy before we finish building our packages, we need to manually run the downloads sync Jenkins script:
    * https://builds.globus.org/jenkins/view/all/job/Synchronize%20GCSv5%20Stable/build?delay=0sec
        * Leave `SYNC_WHEELS_ONLY` unchecked

#### Old Build Instructions

(Previously) As a temporary workaround, we needed to add a few lines to manually set some
env variables in our [JenkinsFile](https://github.com/globus/globus-compute/blob/743fa1e398fd40a00efb5880c55e3fa6e47392fc/compute_endpoint/packaging/JenkinsFile#L24) before triggering the build, as detailed below.

1. Notes
    * Example of unstable repo:
        * https://downloads.globus.org/globus-connect-server/unstable/rpm/el/9/x86_64/
    * Directory of testing images:
        * https://builds.globus.org/downloads.globus.org/globus-connect-server/testing/rpm/el/8/x86_64/
    * Stable repo:
        * The images will be in the below build directory after we finish our build process, but not public:
            * https://builds.globus.org/downloads.globus.org/globus-connect-server/stable/rpm/el/8/x86_64/
        * After GCS push during deploy day (or if we ping them to do so), the public images will be located at:
            * https://downloads.globus.org/globus-connect-server/stable/rpm/el/9/x86_64/
      [publishResults.groovy line 85](https://github.com/globusonline/gcs-build-scripts/blob/168617a0ccbb0aee7b3bee04ee67940bbe2a80f6/vars/publishResults.groovy#L85)
1. (Access on VPN) Click the [build button here](https://builds.globus.org/jenkins/job/BuildGlobusComputeAgentPackages/build?delay=0sec)
1. Wait 20-30 minutes and confirm that the [build is green](https://builds.globus.org/jenkins/job/BuildGlobusComputeAgentPackages/)
1. For production release, we will have finished the build before the GCS
   team, and can notify them that our build is complete.  They then will
   publish all packages when they finish their builds, which includes ours.
