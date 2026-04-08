Changed
^^^^^^^

- Newly created Multi-User Compute endpoints now default to public instead of
  private visibility.  This flag can be toggled via the ``public`` param in
  ``config.yaml``.  *Note* Visibility or the lack thereof is *not* a security
  feature.  This is intended to help with default discoverability only, and
  does not affect access/usage of the endpoint, which remain unchanged,
  configured by identity mapping.
