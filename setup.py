<<<<<<< HEAD
import os
from setuptools import setup, find_packages

# single source of truth for package version
version_ns = {}
<<<<<<< HEAD
with open(os.path.join("funcx_endpoint", "version.py")) as f:
=======
with open(os.path.join("funcx_sdk", "version.py")) as f:
>>>>>>> funcx-sdk-master
    exec(f.read(), version_ns)
version = version_ns['__version__']

setup(
<<<<<<< HEAD
    name='funcx_endpoint',
    version=version,
    packages=find_packages(),
    description='funcX user endpoint to receive and perform tasks from the funcX service.',
=======
    name='funcx_sdk',
    version=version,
    packages=find_packages(),
    description='Python interface and utilities for funcX',
>>>>>>> funcx-sdk-master
    long_description=("funcX SDK contains a Python interface to the funcX "
                      "Service."),
    install_requires=[
        "pandas", "requests", "jsonschema", "globus_sdk", "configobj"
    ],
<<<<<<< HEAD

    entry_points={
        'console_scripts': ['funcx_endpoint = funcx_endpoint.endpoint:main']
    },

=======
>>>>>>> funcx-sdk-master
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
<<<<<<< HEAD
    url="https://github.com/funcx-faas/funcx_endpoint"
=======
    url="https://github.com/funcx-faas/funcx_sdk"
>>>>>>> funcx-sdk-master
=======
from setuptools import setup, find_packages

with open('parsl/version.py') as f:
    exec(f.read())

with open('requirements.txt') as f:
    install_requires = f.readlines()

setup(
    name='parsl',
    version=VERSION,
    description='Simple data dependent workflows in Python',
    long_description='Simple parallel workflows system for Python',
    url='https://github.com/Parsl/parsl',
    author='The Parsl Team',
    author_email='parsl@googlegroups.com',
    license='Apache 2.0',
    download_url='https://github.com/Parsl/parsl/archive/{}.tar.gz'.format(VERSION),
    include_package_data=True,
    packages=find_packages(),
    install_requires=install_requires,
    scripts = ['parsl/executors/high_throughput/process_worker_pool.py',
               'parsl/executors/high_throughput/funcx_worker.py',
               'parsl/executors/extreme_scale/mpi_worker_pool.py',
               'parsl/executors/low_latency/lowlatency_worker.py',
    ],
    extras_require = {
        'monitoring' : ['psutil', 'sqlalchemy', 'sqlalchemy_utils'],
        'aws' : ['boto3'],
        'kubernetes' : ['kubernetes'],
        # Jetstream is deprecated since the interface has not been maintained.
        # 'jetstream' : ['python-novaclient'],
        'extreme_scale' : ['mpi4py'],
        'docs' : ['nbsphinx', 'sphinx_rtd_theme'],
        'google_cloud' : ['google-auth', 'google-api-python-client'],
        'gssapi' : ['python-gssapi'],
        'all' : ['psutil', 'sqlalchemy', 'sqlalchemy_utils',
                 'dash', 'dash-html-components', 'dash-core-components', 'pandas',
                 'boto3',
                 'kubernetes',
                 'mpi4py',
                 'nbsphinx', 'sphinx_rtd_theme',
                 'google-auth', 'google-api-python-client',
                 'python-gssapi']

        },
    classifiers = [
        # Maturity
        'Development Status :: 3 - Alpha',
        # Intended audience
        'Intended Audience :: Developers',
        # Licence, must match with licence above
        'License :: OSI Approved :: Apache Software License',
        # Python versions supported
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    keywords=['Workflows', 'Scientific computing'],
    entry_points={'console_scripts':
      [
       'parsl-globus-auth=parsl.data_provider.globus:cli_run'
      ]}
>>>>>>> squashed-parsl
)
