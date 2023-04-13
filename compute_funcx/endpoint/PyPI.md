# Deprecation Warning

This package is deprecated, and currently just wraps the [Globus Compute Endpoint](https://pypi.org/project/globus-compute-endpoint/) package with funcX names. See [here](https://globus-compute.readthedocs.io/en/latest/funcx_upgrade.html) for instructions on how to upgrade.

# funcX Endpoint

[funcX](https://globus-compute.readthedocs.io/en/latest/) is a distributed Function as a Service (FaaS) platform that enables flexible, scalable, and high performance remote function execution. Unlike centralized FaaS platforms, funcX allows users to execute functions on heterogeneous remote computers, from laptops to campus clusters, clouds, and supercomputers.

This package provides the [funcX Endpoint](https://globus-compute.readthedocs.io/en/latest/endpoints.html) agent — the software which receives user-submitted tasks (functions + arguments) and manages their execution on target machines — in addition to command line tools for managing funcX endpoints.

To submit functions for execution on funcX endpoints, use the companion [funcX SDK](https://pypi.org/project/funcx/) package.
