Design notes
============

Since we are interfacing with a queue like interface, a `get`, `put` interface first comes to mind.
However, in the case of the subscriber, we have the added responsibility to split the get behavior to

* waitFor a message
* hand-off to application
* if success -> ack
  else -> nack

This means that we either need:
* A callback that upon successful handoff raises no error
* Or pass the message to a robust queue that's guaranteed to not lose the message
  - We can simulate this behavior with mp.Queue to start with and perhaps add a sqlite db to back it up
    doing the commit to disk in the critical path is hard, so we have to consider some alt mechanism
    that sends acks in an async fashion.

Note : Following a dev discussion the callback option is dropped. The idea is to extend
the multiprocessing Queue to write messages to disk to guarantee no message is lost.

Results Queue
-------------

The ResultsQueuePublisher is left intentionally simple since the main behavior that we
care about initially is covered, i.e:
* A published message is never lost, so the ACK handler is rather simple.
  If message could not be sent we immediately get an error message.
* Need to determine what the retry behavior here ought to be before, if any.

[TODO] Identify what AUTHZ rules can be enforced on topics against which an endpoint can publish.

* On reconnect, it is safe for multiple endpoints to potentially push results over, since
  we expect result updates to be idempotent.



TaskQueueSubscriber Call chain
------------------------------

connect() --- _on_connection_open() ---> open_channel() ---_on_open_channel() --> setup_exchange
                                                                                       |
queue_bind()<-- _on_queue_declare_ok() -- setup_queue() <--- _on_exchange_declare_ok --+
    |
    +-- _on_bind_ok() --> start_consuming()


Disconnect behavior
-------------------

Who can configure:
^^^^^^^^^^^^^^^^^^
For both task_q and result_q, the expected disconnect behavior is to raise an error,
and disengage so that the endpoint can determine the reconnect procedure. Usually
the creator of the queue sets the HEARTBEAT and BLOCKED_CONNECTION_TIMEOUT, with the
subscriber only following.
* Service side is responsible for creating a direct queue named: `<ENDPOINT_ID>.queue`
* Service side is responsible for creating a queue named `results` and binding it to
  the routing_key: `*.results` where each endpoint should publish only with the
  routing_key = `<ENDPOINT_ID>.results`.

This allows the setting of the heartbeats and timeout_period *only* from the service
side. This probably needs more testing, because ATM we don't have an easy way to
trigger connection loss in the middle of testing.

Disconnect error propagation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The `ResultQueuePublisher` is synchronous, so a publish failure is immediately raised.

The `TaskQueueSubscriber` is asynchronous, and most likely on a separate python process.
To communicate a disconnect/failure, we need an event that allows the parent i.e the
endpoint main loop to detect a fault.

When either of the above components fail, we should go through the following steps:
1. Persist results in the pipes to disk
2. Disconnect from the service
3. Executors continue -- think of this as a clutch disengaging -- the engine continues
4. Attempt endpoint register and reconnect logic
5. Propagate pending results

Failover behavior
^^^^^^^^^^^^^^^^^

Failover is when the endpoint fails/disconnects and connects back to the service.
Essentially, the service side, couldn't know if the newly connecting endpoint is
a duplicate or a remote side reconnecting after a disconnect.

* If a duplicate endpoint or a reconnecting ep publishes results we don't mind
* If a duplicate endpoint connects and starts draining tasks, that's a problem.
   * we use "exclusive" subscriber mode on the task_queue: `<ENDPOINT_ID>.tasks`
     ensuring that only 1 endpoint can be fetching tasks. As a result an older
     connection has to be disconnected before a new connection can be created.


Performance Eval
----------------

Throughput on the tasks pipe:

    .. code-block::
        WARNING  test_task_q:test_task_q.py:280 TTC throughput for 1000 messages at 1B = 7517.50messages/s
        WARNING  test_task_q:test_task_q.py:280 TTC throughput for 1000 messages at 2B = 8482.18messages/s
        WARNING  test_task_q:test_task_q.py:280 TTC throughput for 1000 messages at 4B = 8630.61messages/s
        WARNING  test_task_q:test_task_q.py:280 TTC throughput for 1000 messages at 8B = 8706.41messages/s
        WARNING  test_task_q:test_task_q.py:280 TTC throughput for 1000 messages at 16B = 8095.67messages/s
        WARNING  test_task_q:test_task_q.py:280 TTC throughput for 1000 messages at 32B = 8598.09messages/s
        WARNING  test_task_q:test_task_q.py:280 TTC throughput for 1000 messages at 64B = 8591.80messages/s
        WARNING  test_task_q:test_task_q.py:280 TTC throughput for 1000 messages at 128B = 8503.42messages/s
        WARNING  test_task_q:test_task_q.py:280 TTC throughput for 1000 messages at 256B = 8543.69messages/s
        WARNING  test_task_q:test_task_q.py:280 TTC throughput for 1000 messages at 512B = 8057.96messages/s

Basic latency on tasks pipe:

    .. code-block::
        test_task_q:test_task_q.py:246 Message latencies in milliseconds, min:0.70, max:17.70, avg:2.48




