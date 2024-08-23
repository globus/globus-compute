New Functionality
^^^^^^^^^^^^^^^^^

- The multi-user endpoint now saves user-endpoint standard file streams (aka
  ``stdout`` and ``stderr``) to the UEP's ``endpoint.log``.  This makes it much
  easier to identity implementation missteps that affect the early UEP boot
  process, before the UEP's logging is bootstrapped.
