import os
import re

from setuptools import find_namespace_packages, setup

REQUIRES = [
    # request sending and authorization tools
    "requests>=2.20.0",
    "globus-sdk>=3.14.0,<4",
    # 'websockets' is used for the client-side websocket listener
    "websockets==10.3",
    # dill is an extension of `pickle` to a wider array of native python types
    # pin to the latest version, as 'dill' is not at 1.0 and does not have a clear
    # versioning and compatibility policy
    "dill==0.3.5.1",
    # typing_extensions, so we can use Protocol and other typing features on python3.7
    'typing_extensions>=4.0;python_version<"3.8"',
    # packaging, allowing version parsing
    # set a version floor but no ceiling as the library offers a stable API under CalVer
    "packaging>=21.1",
    "pika>=1.2",
    "funcx-common==0.0.20",
    "tblib==1.7.0",
]
DOCS_REQUIRES = [
    "sphinx<5",
    "furo==2021.09.08",
]

TEST_REQUIRES = [
    "flake8==3.8.0",
    "pytest>=7.2",
    "pytest-mock",
    "coverage",
    # easy mocking of the `requests` library
    "responses",
]
DEV_REQUIRES = TEST_REQUIRES + [
    "pre-commit",
]


def parse_version():
    # single source of truth for package version
    version_string = ""
    version_pattern = re.compile(r'__version__ = "([^"]*)"')
    with open(os.path.join("funcx", "version.py")) as f:
        for line in f:
            match = version_pattern.match(line)
            if match:
                version_string = match.group(1)
                break
    if not version_string:
        raise RuntimeError("Failed to parse version information")
    return version_string


setup(
    name="funcx",
    version=parse_version(),
    packages=find_namespace_packages(include=["funcx", "funcx.*"]),
    description="funcX: High Performance Function Serving for Science",
    install_requires=REQUIRES,
    extras_require={
        "dev": DEV_REQUIRES,
        "test": TEST_REQUIRES,
        "docs": DOCS_REQUIRES,
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
    author="funcX team",
    author_email="labs@globus.org",
    license="Apache License, Version 2.0",
    url="https://github.com/funcx-faas/funcx",
)
