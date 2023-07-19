import os
from pathlib import Path

from setuptools import find_packages, setup

REQUIRES = [
    "globus-compute-endpoint==2.2.4",
]

version_ns = {}
with open(os.path.join("funcx_endpoint", "version.py")) as f:
    exec(f.read(), version_ns)
version = version_ns["__version__"]

directory = Path(__file__).parent
long_description = (directory / "PyPI.md").read_text()

setup(
    name="funcx-endpoint",
    version=version,
    packages=find_packages(),
    description="funcX: High Performance Function Serving for Science",
    long_description=long_description,
    long_description_content_type="text/markdown",
    install_requires=REQUIRES,
    extras_require={},
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
            "funcx-endpoint=globus_compute_endpoint.cli:cli_run_funcx",
            "funcx-interchange"
            "=globus_compute_endpoint.executors.high_throughput.interchange:cli_run",
            "funcx-manager"
            "=globus_compute_endpoint.executors.high_throughput.manager:cli_run",
            "funcx-worker"
            "=globus_compute_endpoint.executors.high_throughput.worker:cli_run",
        ]
    },
    include_package_data=True,
    author="Globus Compute Team",
    author_email="support@globus.org",
    license="Apache License, Version 2.0",
    url="https://github.com/funcx-faas/funcx",
    project_urls={
        "Changelog": "https://globus-compute.readthedocs.io/en/latest/changelog_funcx.html",  # noqa: E501
        "Upgrade to Globus Compute": "https://globus-compute.readthedocs.io/en/latest/funcx_upgrade.html",  # noqa: E501
    },
)
