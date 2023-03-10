# FuncX Wrapper Package

* This package serves as a backwards compatible library for current funcX SDK users who may upgrade to the latest SDK but
does not want to make any changes to existing scripts that references funcx.* classes to use globus_compute_sdk.* instead.

* Accomplished via a variety of import globus_compute_sdk.compute_class_X as FuncX_class_X in __init__.py and other files
