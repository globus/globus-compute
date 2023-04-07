# funcx_endpoint Wrapper Package

* This package serves as a backwards compatible library for current
`funcx-endpoint` users who may upgrade to the latest code but
does not want to make any changes to existing scripts that reference
`funcx_endpoint.*` classes to use `globus_compute_endpoint.*` instead.

* Accomplished via a variety of `import globus_compute_endpoint.compute_class_X as class_X` in `__init__.py` and other files
