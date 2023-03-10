import os

from setuptools import find_packages, setup

compute_endpoint_path = "/Users/lei/glob/funcX/compute_endpoint"

REQUIRES = [
    # "globus-compute-endpoint>=2.0.0",
    f"globus-compute-endpoint @ file://localhost/{compute_endpoint_path}#egg=compute_endpoint"
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
    include_package_data=True,
    author="Globus Compute Team",
    author_email="support@globus.org",
    license="Apache License, Version 2.0",
    url="https://github.com/funcx-faas/funcx",
)
