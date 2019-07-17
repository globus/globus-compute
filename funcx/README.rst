TODOs
=====


Init
----

* Update endpoint init to use the globus sdk auth code.

Start
-----

* Endpoint start to load configs
* Interchange features:
* Command interface to scale_in/out
* Heartbeats with the executor interface
* End-to-end task execution test
* Managers to die if they are unable to make contact with the interchange

Stop
----

* https://github.com/funcx-faas/funcX/issues/32 clean termination
* If the machine reboots, the pidfiles are not cleaned out.
