Changed
^^^^^^^

- The Executor now implements a bursty rate-limit in the background submission
  thread.  The Executor is designed to coalesce up to ``.batch_size`` of tasks
  and submit them in a single API call.  But if tasks are supplied to the
  Executor at just the right frequency, it will send much smaller batches more
  frequently which is "not nice" to the API.  This change allows "bursts" of up
  to 4 API calls in a 16s window, and then will back off to submit every 4
  seconds.  Notes:

  - ``.batch_size`` currently defaults to 128 but is user-settable

  - If the Executor is able to completely fill the batch of tasks sent to the
    API, that call is not counted toward the burst limit
