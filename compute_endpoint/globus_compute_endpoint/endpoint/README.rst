Endpoint UX-Design
------------------

Here are the basic functions we want from the `globus-compute-endpoint`:

* init : One time initialization that sets up the global options for the site
  at which the endpoint is being deployed
* start : Start an endpoint by name. If no valid configuration is available,
  make a default configuration available for the user to edit. If the endpoint
  is configured but not registered, do the registration.
* stop : Stop an endpoint by name.
  TODO : Figure out how to make this work on multi-host login nodes with a shared-fs
* list : List all available endpoints
