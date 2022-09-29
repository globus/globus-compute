New Functionality
^^^^^^^^^^^^^^^^^

- Add a flag to avoid creating websocket queues on batch runs, the new default is not to create.
  Note that if the queue is not created, results will have to be retrieved directly instead of
  via background polling of the websocket
