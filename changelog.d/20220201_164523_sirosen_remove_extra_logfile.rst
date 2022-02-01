Changed
^^^^^^^

- Logging for funcx-endpoint no longer writes to `~/.funcx/endpoint.log` at any point.
  This file is considered deprecated. Use `funcx-endpoint --debug <command>` to
  get debug output written to stderr.
- The output formatting of `funcx-endpoint` logging has changed slightly when
  writing to stderr.
