# Multi-User Compute Endpoints

Multi-user compute endpoints (MEP) enable an administrator to securely offer
their compute resources to their users without mandating the typical shell
(i.e., ssh) access.  Administrators may preset endpoint configuration options
(e.g., Torque, PBS, or Slurm), and also offer user-configurable items (e.g.,
account id, problem-specific number of blocks).

## tl;dnr

For those who just want to use the tool:

### Administrator setup

1. Install the `globus-compute-endpoint` package from PyPI.  (These
   instructions use a basic virtualenv but that's not mandatory; the key detail
   is that the software must be accessible to regular user accounts, so cannot
   be installed in, say, `/root/`.)

    ```console
    # virtualenv /opt/globus-compute-endpoint
    # . /opt/globus-compute-endpoint/bin/activate
    # python -m pip install globus-compute-endpoint
    ```

1. Optionally, create an alias for easier typing in this session:

    ```console
    # alias globus-compute-endpoint=/opt/globus-compute-endpoint/bin/globus-compute-endpoint
    ```

1. Create the Multi-User Endpoint configuration with the `--multi-user` flag
   (currently not advertised in `--help`) to the `configure` subcommand:

    ```console
    # globus-compute-endpoint configure --multi-user prod_gpu_large
    Created multi-user profile for endpoint named <prod_gpu_large>

        Configuration file: /root/.globus_compute/prod_gpu_large/config.yaml

        Example identity mapping configuration: /root/.globus_compute/prod_gpu_large/example_identity_mapping_config.json

        User endpoint configuration template: /root/.globus_compute/prod_gpu_large/user_config_template.yaml
        User endpoint configuration schema: /root/.globus_compute/prod_gpu_large/user_config_schema.json
        User endpoint environment variables: /root/.globus_compute/prod_gpu_large/user_environment.yaml

    Use the `start` subcommand to run it:

        $ globus-compute-endpoint start prod_gpu_large
    ```

1. Setup the identity mapping configuration&nbsp;&mdash;&nbsp;this depends on
   your site's specific requirements and may take some trial and error.  The
   key point is to be able to take a Globus Auth Identity set, and map it to a
   local username _on this resource_&nbsp;&mdash;&nbsp;this resulting username
   will be passed to
   [`getpwnam(3)`](https://www.man7.org/linux/man-pages/man3/getpwnam.3.html)
   to ascertain a UID for the user.  This file is linked in `config.yaml` (from
   the previous step's output), and, per initial configuration, is set to
   `example_identity_mapping_config.json`.  This file has a valid
   configuration, but references `example.com` so will not work until modified.
   Please refer to the [Globus Connect Server Identity Mapping
   Guide](https://docs.globus.org/globus-connect-server/v5.4/identity-mapping-guide/#mapping_recipes)
   for help on updating this file, or reach out in [#help on the Globus Compute
   Slack](https://funcx.slack.com/archives/C017637NZFA).

1. Modify `user_config_template.yaml` as appropriate for the resources you want
   to make available.  This file will be interpreted as a Jinja template and
   will be rendered with user-provided variables to generate the final UEP
   configuration.  The default configuration (as created in step 4) has a basic
   working configuration, but uses the `LocalProvider`.

   Please look to [the documentation for a number of known working
   examples](https://globus-compute.readthedocs.io/en/latest/endpoints.html#example-configurations)
   (all written for single-user use) as a starting point.

1. Optionally modify `user_config_schema.json`; the file, if it exists, defines
   the [JSON schema](https://json-schema.org/) against which user-provided
   variables are validated.  (N.B.: if a variable is not specified in the
   schema but exists in the template, then it is treated as valid.)

1. Modify `user_environment.yaml` for any environment variables you would like
   injected into user endpoints process space:

    ```yaml
    SOME_SITE_SPECIFIC_ENV_VAR: a site specific value
    ```

1. Run MEP manually for testing and easier debugging, as well as to collect the
   (Multi-User) endpoint ID for sharing with users.  The first time through,
   the Globus Compute Endpoint will initiate a Globus Auth login flow, and
   present a long URL:

    ```console
    # globus-compute-endpoint start prod_gpu_large
    > Endpoint Manager initialization
    Please authenticate with Globus here:
    ------------------------------------
    https://auth.globus.org/v2/oauth2/authorize?clie...&prompt=login
    ------------------------------------

    Enter the resulting Authorization Code here: <PASTE CODE HERE AND PRESS ENTER>
    ```

1. While iterating, the `--log-to-console` flag may be useful to emit the log
   lines to the console (also available at
   `.globus_compute/prod_gpu_large/endpoint.log`).

    ```console
    # globus-compute-endpoint start prod_gpu_large --log-to-console
    > Endpoint Manager initialization
    >

    ========== Endpoint Manager begins: 1ed568ab-79ec-4f7c-be78-a704439b2266
            >>> Multi-User Endpoint ID: 1ed568ab-79ec-4f7c-be78-a704439b2266 <<<
    ```

    Additionally, for even noiser output, there is `--debug`.

1. When satisfied, use a `systemd` unit file to [automatically start the
   multi-user endpoint when the host
   boots](https://globus-compute.readthedocs.io/en/latest/endpoints.html#restarting-endpoint-when-machine-restarts):

    ```console
    # cat /etc/systemd/system/globus-compute-endpoint-prod_gpu_large.service
    [Unit]
    Description=Globus Compute Endpoint systemd service (prod_gpu_large)
    After=network.target
    StartLimitIntervalSec=0

    [Service]
    ExecStart=/opt/globus-compute-endpoint/bin/globus-compute-endpoint start prod_gpu_large
    User=root
    Type=simple
    Restart=always
    RestartSec=30

    [Install]
    WantedBy=multi-user.target
    ```

    And enable via the usual interaction:

    ```console
    # systemctl enable globus-compute-endpoint-prod_gpu_large --now
    ```

## Background

A multi-user endpoint (MEP) might be thought of as a user-endpoint (UEP)
manager.  In a typical non-MEP paradigm, a normal user would log in (e.g., via
SSH) to a compute resource (e.g., a cluster's login-node), create a Python
virtual environment (e.g., [virtualenv](https://pypi.org/project/virtualenv/),
[pipx](https://pypa.github.io/pipx/),
[conda](https://docs.conda.io/en/latest/)), and then install and run
`globus-compute-endpoint` from their user-space.  By contrast, a MEP is a
root-installed and root-run process that manages child processes for regular
users.  Upon receiving a "start endpoint" request from the Globus Compute AMQP
service, a MEP creates a user-process via the venerable _fork()_&rarr;_drop
privileges_&rarr;_exec()_ pattern, and then watches that child process until it
stops.  At no point does the MEP ever attempt to execute tasks, nor does the
MEP even see tasks&nbsp;&mdash;&nbsp;those are handled the same as they have
been to-date, by the UEPs.

The workflow for a task sent to an MEP roughly follows these steps:

1. The user acquires an MEP endpoint id (perhaps as shared by the site owner).

1. The user uses the SDK to send the task to the MEP with the `endpoint_id`:

    ```python
    from globus_compute_sdk import Executor

    def some_task(*a, **k):
        return 1

    mep_site_id = "..."  # as acquired from step 1
    with Executor(endpoint_id=mep_site_id) as ex:
        fut = ex.submit(some_task)
        print("Result:", fut.result())  # Reminder: blocks until result received
    ```

1. After the `ex.submit()` call, the SDK POSTs a REST request to the Globus
   Compute web-service.

1. The web-service identifies the endpoint in the request as belonging to a
   MEP.

1. The web-service generates a UEP id specific to the tuple of the
   `mep_site_id`, the id of the user making the request, and the endpoint
   configuration in the request (e.g., `tuple(site_id, user_id,
   conf)`&nbsp;&mdash;&nbsp;this identifier is simultaneously stable and
   unique.

1. The web-service sends a start-UEP-request to the MEP (via AMQP), asking it
   to start an endpoint identified by the id generated in the previous step,
   and as the user identified by the REST request.

1. The MEP maps the Globus Auth identity in the start-UEP-request to a local
   username.

1. The MEP ascertains the host-specific UID based on a
   [`getpwnam(3)`](https://www.man7.org/linux/man-pages/man3/getpwnam.3.html)
   call with the local username from the previous step.

1. The MEP starts a UEP as the UID from the previous step.

1. The just-started UEP checks in with the Globus Compute web-services.

1. The web-services will see the check-in and then complete the original
   request to the SDK, accepting the task and submitting it to the now-started
   UEP.

The above workflow may be of interest to system administrators from a "how does
this work in theory?" point of view, but will be of little utility to most
users.  The part of interest to most end users is the on-the-fly custom
configuration.  If the administrator has provided any hook-in points in
`user_config_template.yaml` (e.g., an account id), then a user may specify that
via the `user_endpoint_config` argument to the Executor constructor:

```python
from globus_compute_sdk import Executor

def some_task(*a, **k):
    return 1

mep_site_id = "..."  # as acquired from step 1
with Executor(
    endpoint_id=mep_site_id,
    user_endpoint_config={"account_id": "user_allocation_account_id"},
) as ex:
    fut = ex.submit(some_task)
    print("Result:", fut.result())  # Reminder: blocks until result received
```

## Key Benefits

### For Administrators

This biggest benefit of a MEP setup is a lowering of the barrier for legitimate
users of a site.  To date, knowledge of the command line has been critical to
most users of High Performance Computing (HPC)) systems, though only as a
necessity of infrastructure rather than a legitimate scientific purpose.  A MEP
allows a user to ignore many of the important-but-not-really details of
plumbing, like logging in through SSH, restarting user-only daemons, or, in the
case of Globus Compute, fine-tuning a scheduler options by managing multiple
endpoint configurations.  The only thing they need to do is run their scripts
locally on their own workstation, and the rest "just works."

Another boon for administrators is the ability to fine-tune and pre-configure
what resources UEPs may utilize.  For example, many users struggle to discover
which interface is routed to a cluster's internal network; the administrator
can preset that, completely bypassing the question.  Using ALCF's Polaris as an
example, the administrator could use the following user configuration template
(`user_config_template.yaml`) to place all jobs sent to this MEP on the
`debug-scaling` queue, and pre-select the obvious defaults ([per the
documentation](https://docs.alcf.anl.gov/polaris/running-jobs/)):

```yaml
display_name: Polaris at ALCF - debug-scaling queue
engine:
  type: GlobusComputeEngine
  address:
    type: address_by_interface
    ifname: bond0

  strategy:
    type: SimpleStrategy
    max_idletime: 30

  provider:
    type: PBSProProvider
    queue: debug-scaling

    account: {{ ACCOUNT_ID }}

    # Command to be run before starting a worker
    # e.g., "module load Anaconda; source activate parsl_env"
    worker_init: {{ WORKER_INIT_COMMAND|default() }}

    init_blocks: 0
    min_blocks: 0
    max_blocks: 1
    nodes_per_block: {{ NODES_PER_BLOCK|default(1) }}

    walltime: 1:00:00

    launcher:
      type: MpiExecLauncher

idle_heartbeats_soft: 10
idle_heartbeats_hard: 5760
```

The user must specify the `ACCOUNT_ID`, and could optionally specify the
`WORKER_INIT_COMMAND` and `NODES_PER_BLOCK` variables.  If the user's jobs
finish and no more work comes in after `max_idletime` seconds (30s), the UEP
will scale down and consume no more wall time.

(Validation of the user-provided variables is available, but not yet
documented.  For the motivated, please look to `user_config_schema.json`.)

Another benefit is a cleaner process table on the login nodes.  Rather than
having user endpoints sit idle on a login-node for days after a run has
completed (perhaps until the next machine reboot), a MEP setup automatically
shuts down idle UEPs (as defined in `user_config_template.yaml`).  When the UEP
has had no movement for 48h (by default; see `idle_heartbeat_hard`), or has no
outstanding work for 5m (by default; see `idle_heartbeats_soft`), it will shut
itself down.

### For Users

Under the MEP paradigm, users largely benefit from not having to be quite so
aware of an endpoint and its configuration.  As the administrator will have
taken care of most of the smaller details (c.f., installation, internal
interfaces, queue policies), the user is able to write a consuming script,
knowing only the endpoint id and their system accounting username:

```python
import concurrent.futures
from globus_compute_sdk import Executor

def jitter_double(task_num):
    import random
    return task_num, task_num * (1.5 + random.random())

polaris_site_id = "..."  # as acquired from the admin in the previous section
with Executor(
    endpoint_id=polaris_site_id,
    user_endpoint_config={
        "ACCOUNT_ID": "user_allocation_account_id",
        "NODES_PER_BLOCK": 2,
    }
) as ex:
    futs = [ex.submit(jitter_double, task_num) for task_num in range(100)]
    for fut in concurrent.futures.as_completed(futs):
        print("Result:", fut.result())
```

It is a boon for the researcher to see the relevant configuration variables
immediately adjacent to the code, as opposed to hidden in the endpoint
configuration and behind an opaque endpoint id.  An MEP removes almost half of
the infrastructure plumbing that the user must manage&nbsp;&mdash;&nbsp;many
users will barely even need to open their own terminal, much less an SSH
terminal on the login node.
