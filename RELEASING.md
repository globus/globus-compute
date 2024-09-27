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

Before building the packages, ensure that the release itself, either the alpha
or prod versions, is published on PyPI.

Additionally, VPN needs to be enabled for the build page.

#### Build Process

In the future, building the DEB/RPM packages will be a simple one-step button
click of the green **Build** button on the Globus Compute Agent
[build page here](https://builds.globus.org/jenkins/job/BuildGlobusComputeAgentPackages/build?delay=0sec).

As a temporary workaround, we need to add a few lines to manually set some
env variables in our [JenkinsFile](https://github.com/globus/globus-compute/blob/743fa1e398fd40a00efb5880c55e3fa6e47392fc/compute_endpoint/packaging/JenkinsFile#L24) before triggering the build, as detailed below.

1. Git checkout both the current release branch that was recently pushed to
   PyPI, ie. ``v2.23.0`` or ``v2.25.0a0`` and the ``build_for_stable`` branch
2. Rebase ``build_for_stable`` on the release branch which should result in
   adding the following ~6 lines:

        ...
              env.BRANCH_NAME = scmVars.GIT_BRANCH.replaceFirst(/^.*origin\//, "")
        +     env.TAG_NAME = sh(returnStdout: true, script: "git tag --contains | head -1").trim()
              env.SOURCE_STASH_NAME = "${UUID.randomUUID()}"
              echo "env.BRANCH_NAME = ${env.BRANCH_NAME}"
              sh "git clean -fdx"

        +     // temporary hack to build for stable
        +     sh "git checkout build_for_stable"
        +     env.TAG_NAME = "v2.23.0"
        +     env.DEFAULT_BRANCH = "build_for_stable"
        +
              dir("compute_endpoint/packaging/") {
        ...

3. Change the ``env.TAG_NAME`` above to the current production release version
    * Note that ``env.TAG_NAME`` determines whether the build is sent to
      the ``unstable`` repo or also to the ``testing`` and ``stable`` ones.
    * Example of unstable repo:
        * https://downloads.globus.org/globus-connect-server/unstable/rpm/el/9/x86_64/
    * Example of stable repo:
        * https://downloads.globus.org/globus-connect-server/stable/rpm/el/9/x86_64/
    * The logic of whether a release is stable is determined by whether the
      package version of Globus Compute Endpoint set in ``version.py`` or
      ``setup.py`` matches ``env.TAG_NAME`` above.   If they are unequal, then
      [publishResults.groovy line 85](https://github.com/globusonline/gcs-build-scripts/blob/168617a0ccbb0aee7b3bee04ee67940bbe2a80f6/vars/publishResults.groovy#L85)
      will be (``tag`` : v2.23.0) != (``stable_tag`` : v2.23.0a0), where
      stable_tag is constructed from the package version of an alpha release.
4. Commit and push your ``build_for_stable`` branch
5. (Access on VPN) Click the [build button here](https://builds.globus.org/jenkins/job/BuildGlobusComputeAgentPackages/build?delay=0sec)
6. Wait 20-30 minutes and confirm that the [build is green](https://builds.globus.org/jenkins/job/BuildGlobusComputeAgentPackages/)
7. For production release, we will have finished the build before the GCS
   team, and can notify them that our build is complete.  They then will
   publish all packages when they finish their builds, which includes ours.
