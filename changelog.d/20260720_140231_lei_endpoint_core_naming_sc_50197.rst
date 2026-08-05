Changed
^^^^^^^

- The Compute "parent" endpoint process responsible for maintaining the lifecycle
  of individual user endpoints has been renamed from the ``Manager Endpoint``
  (MEP) to the ``Core Endpoint`` (CEP).

   This change merely renames classes, variables and updates documentation and
   log output.  No underlying functionality is affected.

   *NOTE* As log output now refer to Core Endpoints instead of Manager Endpoints, any
   scripts that looked for/monitored the exact spelling in logs will have to be updated.

   For more information, please see |CoreEndpoint|_.
