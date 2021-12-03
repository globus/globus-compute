import os

from setuptools import find_namespace_packages, setup

REQUIRES = [
    # request sending and authorization tools
    "requests>=2.20.0",
    "globus-sdk<3",
    # 'websockets' is used for the client-side websocket listener
    "websockets==9.1",
    # table printing used in search result rendering
    "texttable>=1.6.4,<2",
    # versions >=0.2.3 requires globus-sdk v3
    # TODO: update pin to latest when globus-sdk is updated
    "fair_research_login==0.2.2",
    # dill is an extension of `pickle` to a wider array of native python types
    # pin to the latest version, as 'dill' is not at 1.0 and does not have a clear
    # versioning and compatibility policy
    "dill==0.3.4",
]

TEST_REQUIRES = [
    "flake8==3.8.0",
    "numpy",
    "pytest",
]
DEV_REQUIRES = TEST_REQUIRES + [
    "pre-commit",
]

version_ns = {}
with open(os.path.join("funcx", "sdk", "version.py")) as f:
    exec(f.read(), version_ns)
version = version_ns["VERSION"]

setup(
    name="funcx",
    version=version,
    packages=find_namespace_packages(include=["funcx", "funcx.*"]),
    description="funcX: High Performance Function Serving for Science",
    install_requires=REQUIRES,
    extras_require={
        "dev": DEV_REQUIRES,
        "test": TEST_REQUIRES,
    },
    python_requires=">=3.6.0",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: Apache Software License",
        "Natural Language :: English",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Topic :: Scientific/Engineering",
    ],
    scripts=["funcx/serialize/off_process_checker.py"],
    keywords=["funcX", "FaaS", "Function Serving"],
    author="funcX team",
    author_email="labs@globus.org",
    license="Apache License, Version 2.0",
    url="https://github.com/funcx-faas/funcx",
)
