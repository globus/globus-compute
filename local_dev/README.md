# Local dev

Tools for local development of Globus Compute.


## Docker Compose

Globus Compute endpoints are only supported on Linux, so non-Linux users must utilize Docker containers
(or similar) for their local development workflow.

Running `docker compose up -d` in this directory will use `endpoint_dev.Dockerfile`, located in the
project's root directory, to build a Docker image and container with `globus-compute-endpoint` installed.

### Python version

We install Python 3.11 in the Docker image by default, but you can override this via the `PYTHON_VERSION`
environment variable.

### Local user accounts

By default, the Dockerfile references the `USER` environment variable from the host to create a local
user account in the Docker image. You can override this by setting the `LOCAL_USER` environment variable
before building the image.

The local user account will have sudo privileges, allowing you to start endpoints in different contexts.

### Development workflow

We create a bind mount to mount this repo into the container at `/opt/globus-compute`, and run `pip install -e`
for both the `compute_endpoint` and `compute_sdk` directories. In short, this means that the Docker container
will immediately reflect any changes made to the repo on your host machine.

### Accessing the host network

Non-Linux users have limited options to expose their host's IP to a Docker container, which is necessary
when running the Globus Compute services locally. The solution we've chosen is to forward traffic destined
for the container's loopback (i.e., `localhost`) to `host.docker.internal`, the magic Docker-provided DNS
name that resolves to the internal IP of the host. By default, we forward all traffic over port `5000` and
`5672`, but you can modify these via the `WEB_SVC_PORT` and `AMQP_SVC_PORT` environment variables, respectively.

### Persistent data

We enable persistent endpoints by creating bind mounts for the `.globus_compute` directories of the root user
and generated local user. The source directories for these bind mounts can be found in `./docker_data`, which
is created by Docker Compose when initially starting the container.