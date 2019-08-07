Debugging
=========

To sidestep the FuncX web-service the endpoint can be forced to connect directly to a debug-forwarder.
To enable this behavior update `~/.funcx/config` ::


  * 'broker_address' : Address at which the broker is listening, eg: "http://127.0.0.1:50005"
  * 'broker_test' : True
  * 'redis_host' : Point redis host to a locally running redis server. Eg: "127.0.0.1"
