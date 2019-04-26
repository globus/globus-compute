import os
from setuptools import setup, find_packages

# single source of truth for package version
version_ns = {}
with open(os.path.join("funcx_sdk", "version.py")) as f:
    exec(f.read(), version_ns)
version = version_ns['__version__']

setup(
    name='funcx_sdk',
    version=version,
    packages=find_packages(),
    description='Python interface and utilities for funcX',
    long_description=("funcX SDK contains a Python interface to the funcX "
                      "Service."),
    install_requires=[
        "pandas", "requests", "jsonschema", "globus_sdk", "configobj"
    ],
    python_requires=">=3.4",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: Apache Software License",
        "Natural Language :: English",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Topic :: Scientific/Engineering"
    ],
    keywords=[
        "funcX",
        "FaaS",
        "Function Serving"
    ],
    author='Ryan Chard',
    author_email='rchard@anl.gov',
    license="Apache License, Version 2.0",
    url="https://github.com/funcx-faas/funcx_sdk"
)
