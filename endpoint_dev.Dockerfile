ARG PYTHON_VERSION="3.11"
FROM python:${PYTHON_VERSION}

RUN apt-get update && apt-get upgrade -y
RUN apt-get install -y less socat sudo vim

RUN mkdir -p /opt/globus-compute
COPY . /opt/globus-compute

RUN python -m pip install -U pip
RUN python -m pip install -e /opt/globus-compute/compute_endpoint
RUN python -m pip install -e /opt/globus-compute/compute_sdk

ARG LOCAL_USER
RUN useradd -m ${LOCAL_USER}
RUN usermod -aG sudo ${LOCAL_USER}
RUN echo "${LOCAL_USER} ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers.d/${LOCAL_USER}
USER ${LOCAL_USER}
ENV HOME /home/${LOCAL_USER}
WORKDIR /home/${LOCAL_USER}
