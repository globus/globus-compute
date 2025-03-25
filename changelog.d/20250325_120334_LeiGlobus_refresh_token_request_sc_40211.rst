Bug Fixes
^^^^^^^^^

- Refresh tokens were not being requested properly since 2.28.0 when going through login flow.  Now they are requested as expected.  This previously prevented auto refreshing of access tokens upon expiry.
