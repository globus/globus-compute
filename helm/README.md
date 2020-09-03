# Kubernetes Endpoint Helm Chart
This chart will deploy a functioning Kubernetes endpoint into your cluster. It
will launch workers with a specified container image into a namespace.

It uses Role Based Access Control to create a service account and assign it
permissions to create the worker pod.

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
