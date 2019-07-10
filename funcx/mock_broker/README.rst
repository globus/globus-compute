Notes
=====


We want the mock_broker to be hosting a REST service. This service will have the following routes:

/register
---------

This route expects a POST with a json payload that identifies the endpoint info and responds with a
json response.

For eg:

POST payload::

  {
     'python_v': '3.6',
     'os': 'Linux',
     'hname': 'borgmachine2',
     'username': 'yadu',
     'funcx_v': '0.0.1'
  }


Response payload::

  {
     'endpoint_id': endpoint_id,
     'task_url': 'tcp://55.77.66.22:50001',
     'result_url': 'tcp://55.77.66.22:50002',
     'command_port': 'tcp://55.77.66.22:50003'
  }




Architecture and Notes
----------------------

The endpoint registers and receives the information
 
```
                                       TaskQ ResultQ
                                          |    |
REST       /register--> Forwarder----->Executor Client
             ^                            |    ^
             |                            |    |
             |                            v    |
             |          +-------------> Interchange
User ----> Endpoint ----|
                        +--> Provider
```
