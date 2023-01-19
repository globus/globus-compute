import os

from setuptools import find_packages, setup

REQUIRES = [
    "requests>=2.20.0,<3",
    "globus-sdk",  # version will be bounded by `funcx`
    "funcx>=1.0.8a",
    "funcx-common==0.0.23",
    # table printing used in list-endpoints
    "texttable>=1.6.4,<2",
    # although psutil does not declare itself to use semver, it appears to offer
    # strong backwards-compatibility promises based on its changelog, usage, and
    # history
    #
    # TODO: re-evaluate bound after we have an answer of some kind from psutil
    # see:
    #   https://github.com/giampaolo/psutil/issues/2002
    "psutil<6",
    # provides easy daemonization of the endpoint
    "python-daemon>=2,<3",
    # CLI parsing
    "click>=8,<9",
    # disallow use of 22.3.0; the whl package on some platforms causes ZMQ issues
    #
    # NOTE: 22.3.0 introduced a patched version of libzmq.so to the wheel packaging
    # which may be the source of the problems , the problem can be fixed by
    # building from source, which may mean there's an issue in the packaged library
    # further investigation may be needed if the issue persists in the next pyzmq
    # release
    "pyzmq>=22.0.0,!=22.3.0,<=23.2.0",
    # 'parsl' is a core requirement of the funcx-endpoint, essential to a range
    # of different features and functions
    # pin exact versions because it does not use semver
    "parsl>=2022.10.4",
    "pika>=1.2.0",
]

TEST_REQUIRES = [
    "responses",
    "pytest>=7.2",
    "coverage>=5.2",
    "pytest-mock==3.2.0",
    "pyfakefs",
]


version_ns = {}
with open(os.path.join("funcx_endpoint", "version.py")) as f:
    exec(f.read(), version_ns)
version = version_ns["__version__"]

setup(
    name="funcx-endpoint",
    version=version,
    packages=find_packages(),
    description="funcX: High Performance Function Serving for Science",
    install_requires=REQUIRES,
    extras_require={
        "test": TEST_REQUIRES,
    },
    python_requires=">=3.7",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: Apache Software License",
        "Natural Language :: English",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Topic :: Scientific/Engineering",
    ],
    keywords=["funcX", "FaaS", "Function Serving"],
    entry_points={
        "console_scripts": [
            "funcx-endpoint=funcx_endpoint.cli:cli_run",
            "funcx-interchange"
            "=funcx_endpoint.executors.high_throughput.interchange:cli_run",
            "funcx-manager"
            "=funcx_endpoint.executors.high_throughput.funcx_manager:cli_run",
            "funcx-worker"
            "=funcx_endpoint.executors.high_throughput.funcx_worker:cli_run",
        ]
    },
    include_package_data=True,
    author="funcX team",
    author_email="labs@globus.org",
    license="Apache License, Version 2.0",
    url="https://github.com/funcx-faas/funcx",
)
