Changed
^^^^^^^

- The Compute "parent" endpoint process, responsible for maintaining the lifecycle
  of individual user endpoints, has been renamed from the ``Manager Endpoint``
  (MEP) to the ``Core Endpoint`` (CEP).

  This change merely renames classes, variables and updates documentation and
  log output.  No underlying functionality is affected.

  For more information, please see the :ref:`Endpoint User Guide <endpoint_user_guide_overview>`

  .. note:: Log output now uses "Core" or "CEP" in place of "Manager" and
            "MEP".  Scripts that relied on finding these exact strings may
            need to be updated.
