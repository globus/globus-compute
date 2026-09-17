Bug Fixes
^^^^^^^^^

- Following up to 4.17.0's credential rotation implementation (long-running
  UEPs able to reconnect to the AMQP service), add support for environments
  with very old glibc (< `v2.27, ~2018`__).

.. __: https://sourceware.org/legacy-ml/libc-announce/2018/msg00000.html
