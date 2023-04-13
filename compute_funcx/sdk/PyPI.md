# Deprecation Warning

This package is deprecated, and currently just wraps the [Globus Compute SDK](https://pypi.org/project/globus-compute-sdk/) package with funcX names. See [here](https://globus-compute.readthedocs.io/en/latest/funcx_upgrade.html) for instructions on how to upgrade.

# funcX SDK

[funcX](https://globus-compute.readthedocs.io/en/latest/) is a distributed Function as a Service (FaaS) platform that enables flexible, scalable, and high performance remote function execution. Unlike centralized FaaS platforms, funcX allows users to execute functions on heterogeneous remote computers, from laptops to campus clusters, clouds, and supercomputers.

This package contains the Python SDK for interacting with funcX. Notable functionality includes submitting functions to remote compute endpoints via the [executor](https://globus-compute.readthedocs.io/en/latest/executor.html), and querying endpoint status.

To manage your own compute endpoints, use the companion [funcX Endpoint](https://pypi.org/project/funcx-endpoint/) package.
