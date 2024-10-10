Changed
^^^^^^^

- Prevent unintended hogging of resources (e.g., login nodes) by setting the default
  endpoint configuration (which uses |LocalProvider|_) to only use a single worker
  (``max_workers_per_node=1``).
