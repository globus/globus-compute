Serialization Strategies
========================

.. autoclass:: globus_compute_sdk.serialize.SerializationStrategy
    :members:
    :member-order: bysource

.. note::
    In the tables below, the ✅ symbol means that the given strategy supports the given
    scenario, while the ❌ symbol means the given strategy cannot support the given
    scenario. Note that even when the ✅ symbol is present, Python serialization is
    tricky and there may be edge cases where the given strategy will fail; if in doubt,
    test a strategy against a payload using :func:`~globus_compute_sdk.serialize.ComputeSerializer.check_strategies`.

.. warning::
    Some code serialization strategies make a best-effort attempt to work across Python
    version boundaries, as shown via the ⚠️ symbol. While they may work in many
    situations, there is no guarantee they will work in every situation or between
    every pair of versions. However, using the same Python version on the submit side
    and execution side *is* guaranteed to work, and is the recommended approach whenever
    possible.

.. this is just for strategies, so any non-strategy exports should be excluded
.. automodule:: globus_compute_sdk.serialize
    :members:
    :member-order: bysource
    :show-inheritance:
    :exclude-members: ComputeSerializer, AllowlistWildcard, SerializationStrategy, DEFAULT_STRATEGY_CODE, DEFAULT_STRATEGY_DATA

.. replacements for docstrings
.. |dill.dumps| replace:: ``dill.dumps()``
.. _dill.dumps: https://dill.readthedocs.io/en/latest/index.html#basic-usage
.. |dill.getsource| replace:: ``dill.source.getsource()``
.. _dill.getsource: https://dill.readthedocs.io/en/latest/dill.html#dill.source.getsource
