FROM --platform=linux/amd64 ubuntu:latest

# don't prompt for location when installing tzdata (indirect dependency of dh-python)
ENV DEBIAN_FRONTEND=noninteractive DEBCONF_NONINTERACTIVE_SEEN=true

RUN apt-get update && apt-get install -y curl

RUN curl -LOs https://downloads.globus.org/globus-connect-server/stable/installers/repo/deb/globus-repo_latest_all.deb
RUN dpkg -i globus-repo_latest_all.deb && rm globus-repo_latest_all.deb

RUN apt-get update && apt-get install -y debhelper dpkg-dev dh-exec dh-python make git globus-python313
