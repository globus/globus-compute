Debugging
=========

To sidestep the FuncX web-service the endpoint can be forced to connect directly to a debug-forwarder.
To enable this behavior update ``~/.funcx/config`` ::


  * 'broker_address' : Address at which the broker is listening, eg: "http://127.0.0.1:50005"
  * 'broker_test' : True
  * 'redis_host' : Point redis host to a locally running redis server. Eg: "127.0.0.1"

Setting up the forwarder locally
--------------------------------

You can run the forwareder-service in debug mode on your local system and skip the web-service entirely.
For this, make sure you have the redis package installed and running. You can check this by running:

>>> redis-cli

This should output a prompt that says : 127.0.0.1:6379. This string needs to match.

Now, you can start the forwarder service for testing:

>>> forwarder-service --address 127.0.0.1 --port 50005 --debug

Once you have this running, we can update the endpoint configs to point to this local service.
