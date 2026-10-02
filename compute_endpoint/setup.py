import os
from pathlib import Path

from setuptools import find_packages, setup

REQUIRES = [
    "globus-sdk",  # version will be bounded by `globus-compute-sdk`
    "globus-compute-sdk==4.18.0a0",
    "globus-identity-mapping==0.5.0",
    # although psutil does not declare itself to use semver, it appears to offer
    # strong backwards-compatibility promises based on its changelog, usage, and
    # history
    #
    # TODO: re-evaluate bound after we have an answer of some kind from psutil
    # see:
    #   https://github.com/giampaolo/psutil/issues/2002
    "psutil>5.9,<8",
    # provides easy daemonization of the endpoint
    "python-daemon>=2,<3",
    # CLI parsing
    "click>=8.3.3,<8.4.0",
    "click-option-group>=0.5.6,<1",
    "pyzmq>=26,<=28",
    "parsl>=2026.7.27",
    "pyprctl<0.2.0",
    "setproctitle>=1.3.2,<1.4",
    "pyyaml>=6.0,<7.0",
    "jinja2>=3.1.6,<3.2",
    "jsonschema>=4.22,<5",
    "cachetools>=6",
    "types-cachetools>=6.0.0.20250525",
    "cryptography>=50",
]

TEST_REQUIRES = [
    "responses",
    "pytest>=9.1",
    "coverage>=7.16",
    "pytest-mock>=3.16",
    "pyfakefs<5.9.2",  # 5.9.2 (Jul 30, 2025), breaks us; retry after 6.0.0 lands?
]


version_ns = {}
with open(os.path.join("globus_compute_endpoint", "version.py")) as f:
    exec(f.read(), version_ns)
version = version_ns["__version__"]

directory = Path(__file__).parent
long_description = (directory / "PyPI.md").read_text()

setup(
    name="globus-compute-endpoint",
    version=version,
    packages=find_packages(),
    description="Globus Compute: High Performance Function Serving for Science",
    long_description=long_description,
    long_description_content_type="text/markdown",
    install_requires=REQUIRES,
    extras_require={
        "test": TEST_REQUIRES,
    },
    python_requires=">=3.10",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Science/Research",
        "Natural Language :: English",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Topic :: Scientific/Engineering",
    ],
    keywords=["Globus Compute", "FaaS", "Function Serving"],
    entry_points={
        "console_scripts": [
            "globus-compute-endpoint=globus_compute_endpoint.cli:cli_run",
            "gce=globus_compute_endpoint.cli:cli_run",
        ]
    },
    include_package_data=True,
    author="Globus Compute Team",
    author_email="support@globus.org",
    license="Apache-2.0",
    url="https://github.com/globus/globus-compute",
    project_urls={
        "Changelog": "https://globus-compute.readthedocs.io/en/latest/changelog.html",  # noqa: E501
        "Upgrade to Globus Compute": "https://globus-compute.readthedocs.io/en/latest/funcx_upgrade.html",  # noqa: E501
    },
)
