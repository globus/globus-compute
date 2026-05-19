ARG PYTHON_VERSION="3.11"
FROM python:${PYTHON_VERSION}

RUN apt-get update && apt-get upgrade -y
RUN apt-get install -y less socat sudo vim

RUN mkdir -p /opt/globus-compute
COPY . /opt/globus-compute

ARG GLOBUS_COMPUTE_COMMON_DIR=""
RUN --mount=type=bind,from=globus_compute_common,source=.,target=/build-context-common,readonly \
	if [ -n "${GLOBUS_COMPUTE_COMMON_DIR}" ]; then \
		if [ ! -d "/build-context-common" ] || [ -z "$(ls -A /build-context-common)" ]; then \
			echo "Path not found or empty: ${GLOBUS_COMPUTE_COMMON_DIR}"; \
			exit 1; \
		fi; \
		mkdir -p /opt/globus-compute-common; \
		cp -R /build-context-common/. /opt/globus-compute-common; \
	fi

RUN python -m pip install -U pip
RUN python -m pip install tox
RUN python -m pip install -e /opt/globus-compute/compute_endpoint
RUN python -m pip install -e /opt/globus-compute/compute_sdk
RUN if [ -n "${GLOBUS_COMPUTE_COMMON_DIR}" ]; then python -m pip install -e /opt/globus-compute-common; fi

ARG LOCAL_USER
RUN useradd -m ${LOCAL_USER}
RUN usermod -aG sudo ${LOCAL_USER}
RUN echo "${LOCAL_USER} ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers.d/${LOCAL_USER}
USER ${LOCAL_USER}
ENV HOME /home/${LOCAL_USER}
WORKDIR /home/${LOCAL_USER}
