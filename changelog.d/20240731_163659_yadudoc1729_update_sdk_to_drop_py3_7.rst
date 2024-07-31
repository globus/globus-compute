Deprecated
^^^^^^^^^^

- ``globus-compute-sdk`` and ``globus-compute-endpoint`` drop support for Python3.7.
  Python3.7 reached `end-of-life on 2023-06-27 <https://devguide.python.org/versions/>`_. We discontinue support for
  Python3.7 since Parsl, an upstream core dependency, has also dropped support for
  it (in ``parsl==2024.7.1``).
