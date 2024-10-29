New Functionality
^^^^^^^^^^^^^^^^^

- Implement optional PAM capabilities for ensuring user accounts meet
  site-specific criteria before starting user endpoints.  Within the multi user
  endpoint, PAM defaults to off, but is enabled via the ``pam`` field:

  .. code-block:: yaml
     :caption: ``config.yaml`` -- Example MEP configuration opting-in to PAM

     multi_user: true
     pam:
       enable: true

  As authentication is implemented via Globus Auth and identity mapping, the
  Globus Compute Endpoint does not implement the authorization or password
  managment phases of PAM.  It implements account
  (|pam_acct_mgmt(3)|_) and session (|pam_open_session(3)|) management.

  For more information, consult :ref:`the PAM section <pam>` of the
  documentation.

  .. |pam_acct_mgmt(3)| replace:: ``pam_acct_mgmt(3)``
  .. _pam_acct_mgmt(3): https://www.man7.org/linux/man-pages/man3/pam_acct_mgmt.3.html
  .. |pam_open_session(3)| replace:: ``pam_open_session(3)``
  .. _pam_open_session(3): https://www.man7.org/linux/man-pages/man3/pam_open_session.3.html

