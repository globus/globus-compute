.. A new scriv changelog fragment.
..
.. Uncomment the header that is right (remove the leading dots).
..
New Functionality
^^^^^^^^^^^^^^^^^

- Teach the endpoint to include the Python and Dill versions, as metadata to Result objects, as well as other useful fields. If the task execution fails, the SDK will use the metadata to highlight differing versions as a possible cause.
