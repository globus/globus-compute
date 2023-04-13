# Globus Compute Endpoint

[Globus Compute](https://globus-compute.readthedocs.io/en/latest/) is a distributed Function as a Service (FaaS) platform that enables flexible, scalable, and high performance remote function execution. Unlike centralized FaaS platforms, Globus Compute allows users to execute functions on heterogeneous remote computers, from laptops to campus clusters, clouds, and supercomputers.

This package provides the [Compute Endpoint](https://globus-compute.readthedocs.io/en/latest/endpoints.html) agent — the software which receives user-submitted tasks (functions + arguments) and manages their execution on target machines — in addition to command line tools for managing compute endpoints.

To submit functions for execution on compute endpoints, use the companion [Globus Compute SDK](https://pypi.org/project/globus-compute-sdk/) package.
