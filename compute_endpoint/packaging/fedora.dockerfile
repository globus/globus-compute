FROM --platform=linux/amd64 fedora:latest

RUN dnf install -y https://downloads.globus.org/globus-connect-server/stable/installers/repo/rpm/globus-repo-latest.noarch.rpm

RUN dnf install -y make rpm-build git globus-python313
