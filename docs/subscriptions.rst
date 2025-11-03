Subscriptions
#############

What is a Subscription?
***********************

A `Globus subscription`_ is a paid, organization-level plan that enables additional
capabilities across the entire `Globus`_ platform.  It is not a personal account; the
subscription is purchased by an institution, research group, or company and applies to
all users in that organization.

While an `organizational subscription`_ is an add-on, many Globus features are
available without one.  For example, the core of Globus Compute |nbsp| --- |nbsp|
Compute endpoints and task submissions |nbsp| --- |nbsp| requires only that users
authenticate with `Globus Auth`_.

.. _Globus: https://www.globus.org/
.. _Globus subscription: https://www.globus.org/subscriptions
.. _organizational subscription: https://www.globus.org/why-subscribe
.. _Globus Auth: https://docs.globus.org/api/auth/


Subscription Benefits
*********************

* Endpoints associated with an institutional subscription may be monitored with the
  `Globus web-console`_.

* Endpoints may have :ref:`multiple administrators <multiple-ep-admins>`

.. _Globus web-console: https://app.globus.org/console/compute


High-Assurance (HA) Subscription Benefits
*****************************************

`High-Assurance`_ ("HA") refers to enhanced security protocols and more stringent
authentication measures related to the specific Globus product.  HA subscriptions
are typically for those organizations who need extra assurances because of the nature
of the data their members process.  For example, Protected Health Information (PHI),
Personally Identifiable Information (PII), and Controlled Unclassified Information
(CUI).  Compute implements the following features when associated with a HA
subscription:

* HA policies -- policies that require extra authentication requirements.  For
  example, authentication with specific (not linked) identities, periodic
  re-authentication, multi-factor authentication (MFA).

* :ref:`ha-functions` -- functions that may only be used by specific HA endpoints
  and that will be removed after 3 months of inactivity.  (SDK access to the HA
  functions is similarly governed by the associated HA policy.)

* :ref:`audit-logging` -- log the events of each task (started/completed, which
  user, etc.)

* in-code assertions that verify endpoint settings are secure (e.g., encryption)


.. tip::

   While use of HA policies requires an HA-enabled subscription, regular (non-HA)
   policies do not require a subscription, HA or otherwise.


.. _High-Assurance: https://docs.globus.org/guides/overviews/security/high-assurance-overview/
.. _audit-logging: http://localhost:12345/endpoints/endpoints.html#audit-logging

.. |nbsp| unicode:: 0xA0
   :trim:
