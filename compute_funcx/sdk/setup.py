import os
import re
from pathlib import Path

from setuptools import find_packages, setup

REQUIRES = [
    "globus-compute-sdk==2.2.4",
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


directory = Path(__file__).parent
long_description = (directory / "PyPI.md").read_text()


setup(
    name="funcx",
    version=parse_version(),
    packages=find_packages(),
    description="Globus Compute: High Performance Function Serving for Science",
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
    keywords=["funcX", "FaaS", "Function Serving", "Globus Compute"],
    author="The Globus Compute Team",
    author_email="support@globus.org",
    license="Apache License, Version 2.0",
    url="https://github.com/funcx-faas/funcx",
    project_urls={
        "Changelog": "https://globus-compute.readthedocs.io/en/latest/changelog_funcx.html",  # noqa: E501
        "Upgrade to Globus Compute": "https://globus-compute.readthedocs.io/en/latest/funcx_upgrade.html",  # noqa: E501
    },
)
