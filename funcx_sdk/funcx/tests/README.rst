Using the test-suite
====================

This test-suite is built using the `pytest` package. Please refer `here <https://docs.pytest.org/en/stable/>`_ for pytest specific docs.


Installing and setting up
-------------------------

First clone the funcx repo:

.. code-block:: bash
		
   git clone https://github.com/funcx-faas/funcX.git
   git checkout forwarder_rearch_1
   cd funcX


Here's a sequence of steps that should be copy-pastable:

.. code-block:: bash
		
    conda create -y --name funcx_testing_py3.8 python=3.8
    conda activate funcx_testing_py3.8
    pip install ./funcx_sdk/
    pip install ./funcx_endpoint/
    pip install -r ./funcx_sdk/test-requirements.txt

Setup an endpoint
-----------------

1. Configure an endpoint:

   >>> funcx-endpoint configure test_local

2. Update the config, by opening up the `~/.funcx/test_local/config.py` file and updating the following:

   >>> # funcx_service_address='https://api.funcx.org/v1'                          
   >>> funcx_service_address="http://k8s-dev.funcx.org/api/v1",

3. Start an endpoint

   >>> funcx-endpoint start test_local

4. Grab the endpoint UUID that's reported!
   
Run tests
---------

1. Go to the tests dir:

   >>> cd funcX/funcx_sdk/funcx/tests
   
2. Run individual tests with:

   >>> pytest test_basic.py --service-address="http://k8s-dev.funcx.org/api/v1" --endpoint="<ENDPOINT_UUID>"

   or, run the whole test-suite with:

   >>> pytest . --service-address="http://k8s-dev.funcx.org/api/v1" --endpoint="<ENDPOINT_UUID>"

   set the funcX service address with:

   >>> pytest . --service-address='<FUNCX_SERVICE_ADDRESS>'

   Example:

   >>> pytest . --endpoint='4b116d3c-1703-4f8f-9f6f-39921e5864df' --service-address='https://api.funcx.org/v1'

4. Follow instructions from `Parsl config documentation <https://parsl.readthedocs.io/en/stable/userguide/configuring.html>` to create a config
   that matches the site you are testing on, and re-run the tests. Once done, please add your configs to the test-issue on github and docs.
   

