Bug Fixes
^^^^^^^^^

- FuncXExecutor no longer ignores the specified ``container_id``.  The same
  function may now be utilized in containers via the normal workflow:

    .. code-block:: python
        import funcx

        def some_func():
            return 1
        with funcx.FuncXExecutor() as fxe:
            fxe.endpoint_id = "some-endpoint-uuid"
            fxe.container_id = "some-container_uuid"
            fxe.submit(some_func)
            fxe.container_id = "some-other-container-uuid"
            fxe.submit(some_func)  # same function, different container!
            # ...
