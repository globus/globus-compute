#!/bin/bash

docker build -f ./Dockerfile-endpoint . -t funcx/kube-endpoint:benc

docker build -f ./Dockerfile-worker . -t funcx/kube-worker:benc

