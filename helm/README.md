# Kubernetes Endpoint Helm Chart
This chart will deploy a functioning Kubernetes endpoint into your cluster. It
will launch workers with a specified container image into a namespace.

It uses Role Based Access Control to create a service account and assign it
permissions to create the worker pod.

## How to Use
First you need to install valid funcX credentials into the cluster's 
namespace. Launch a local version of the endpoint to get it to populate your
`~/.funcx/credentials/funcx_sdk_tokens.json` with
```
funcx-endpoint start 
```

It will prompt you with an authentication URL to visit and ask you to paste the
resulting token. After it completes you can stop your endpoint with 
```
funcx-endpoint stop 
```

cd to your `~/.funcx/credentials` directory and install the keys file as a
kubernetes secret.

```shell script
kubectl create secret generic funcx-sdk-tokens --from-file=funcx_sdk_tokens.json
```

### Install the helm chart
You need to add the funcx helm chart repo to your helm

```shell script
repo add funcx http://funcx.org/funcx-helm-charts/
repo update
```

Create a local values.yaml file to set any specific values you wish to
override.

Then invoke the chart installation with:

```shell script
helm install -f covid19-mesa-values.yaml funcx funcx/funcx_endpoint
```

Once the pods start you can view your endpoint UID with 


The notes that are printed with the installation will tell you how to access the
logs for the endpoint to see the UID.


## Values
The deployment is configured via values.yaml file.

| Value | Description | Default |
|-------| ----------- | ------- |
| funcXServiceAddress | URL for the FuncX Webservice. | https://api.funcx.org |
| image.repository | Docker image repository |  funcx/kube-endpoint |
| image.tag | Tag name for the endpoint image | endpoint_helm |
| image.pullPolicy | Pod pull policy for the endpoint image |  Always |
| workerImage | Docker image to run in the worker pods |  python:3.6-buster |
| workerInit | Command to execute on worker before strating uip | pip install parsl==0.9.0;pip install --force-reinstall funcx>=0.0.2a0 |
| workerNamespace | Kubernetes namespace to launch worker pods into | default |
| workingDir | Directory inside the container where log files are to be stored | /tmp/worker_logs |
| rbacEnabled | Create service account and roles? | true |
| initMem | Initial memory for worker pod | 2000Mi |
| maxMem| Maximum allowed memory for worker pod | 16000Mi |
| initCPU | Initial CPUs to allocate to worker pod | 1 |  
| maxCPU | Maximum CPUs to allocate to worker pod | 2 |
| maxBlocks | Maximum number of worker pods to spawn | 100 |
