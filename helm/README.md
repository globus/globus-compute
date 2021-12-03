# Kubernetes Endpoint Helm Chart
This chart will deploy a functioning Kubernetes endpoint into your cluster. It
will launch workers with a specified container image into a namespace.

It uses Role Based Access Control to create a service account and assign it
permissions to create the worker pod.

## How to Use
First you need to install valid funcX credentials into the cluster's
namespace. If you've used the funcX client, these will already be available
in your home directory's `.funcx/credentials` folder. If not, they can easily
be created with:
```shell
pip install funcx
python -c "from funcx.sdk.client import FuncXClient; FuncXClient()"
````
It will prompt you with an authentication URL to visit and ask you to paste the
resulting token.

Now that you have a valid funcX token, cd to your `~/.funcx/credentials`
directory and install the keys file as a kubernetes secret.

```shell script
kubectl create secret generic funcx-sdk-tokens --from-file=funcx_sdk_tokens.json
```

### Install the helm chart
You need to add the funcx helm chart repo to your helm

```shell script
helm repo add funcx http://funcx.org/funcx-helm-charts/ && helm repo update
```

Create a local values.yaml file to set any specific values you wish to
override. Then invoke the chart installation by specifying your custom values,
the funcx helm repo to install from, and the funcx-endpoint chart:

```shell script
helm install -f your-values.yaml funcx funcx/funcx_endpoint
```

The notes that are printed with the installation will tell you how to access the
logs for the endpoint to see the UID. For example, to retrieve the pod name use
the following command:

```shell script
export EP_POD_NAME=$(kubectl get pods --namespace default -l "app=funcx-endpoint" -o jsonpath="{.items[0].metadata.name}")
```

---
**NOTE**

Depending on your configuration, the namespace may be something other than
'default'.

---

And view the pod's status via:

```shell script
kubectl get pods $EP_POD_NAME
```

Or its logs via:

```shell script
kubectl logs $EP_POD_NAME
```

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
| maxWorkersPerPod | How many workers will be scheduled in each pod | 1 |
| detachEndpoint | Run the endpoint as a daemon inside the pod? | true |
| endpointUUID   | Specify an existing UUID to this endpoint. Leave blank to generate a new one | |

