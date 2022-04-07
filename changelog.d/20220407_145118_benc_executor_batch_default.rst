Changed
^^^^^^^

- FuncXExecutor will use batched submission by default. This can lead to significant
  improvements in task submission rate when using the executor interface (for example,
  3 seconds to submit 500 tasks vs 2 minutes, in an informal test). However,
  individual task submission latency may be increased.

  To use non-batched submission mode, set batch_mode=False when creating FuncXExecutor.

