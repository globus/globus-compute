Changed
^^^^^^^

- Updated minimum Globus SDK requirement to v3.51.0

- Implement use of the |/v3/functions|_ API route in the Client.  For most
  users, this will be a transparent change.  However, for those who manually
  construct the serialized function code, this will necessitate also now
  specifying the function metadata, including the new serializer identifier.
