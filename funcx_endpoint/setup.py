import os
from setuptools import setup, find_packages

version_ns = {}
with open(os.path.join("funcx_endpoint", "version.py")) as f:
    exec(f.read(), version_ns)
version = version_ns['VERSION']

with open('requirements.txt') as f:
    install_requires = f.readlines()

setup(
    name='funcx-endpoint',
    version=version,
    packages=find_packages(),
    description='funcX: High Performance Function Serving for Science',
    install_requires=install_requires,
    python_requires=">=3.6.0",
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
    entry_points={'console_scripts':
                  ['funcx-endpoint=funcx_endpoint.endpoint.endpoint:cli_run',
                   'funcx-interchange=funcx_endpoint.executors.high_throughput.interchange:cli_run',
                   'funcx-manager=funcx_endpoint.executors.high_throughput.funcx_manager:cli_run',
                   'funcx-worker=funcx_endpoint.executors.high_throughput.funcx_worker:cli_run',
                  ]
    },
    include_package_data=True,
    author='funcX team',
    author_email='labs@globus.org',
    license="Apache License, Version 2.0",
    url="https://github.com/funcx-faas/funcx"
)
