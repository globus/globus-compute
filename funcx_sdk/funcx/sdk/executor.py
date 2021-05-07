import time
import threading
import os
import uuid
import sys
from concurrent.futures import Future
import concurrent
import logging
import asyncio
import websockets
import json
import dill
from websockets.exceptions import InvalidHandshake
import multiprocessing as mp
import atexit

logger = logging.getLogger(__name__)


class FuncXExecutor(concurrent.futures.Executor):
    """ An executor
    """

    def __init__(self, funcx_client,
                 results_ws_uri: str = 'ws://localhost:6000',
                 label: str = 'FuncXExecutor',
                 auto_start: bool = True):
        """
        Parameters
        ==========

        funcx_client : client object
            Instance of FuncXClient to be used by the executor

        results_ws_uri : str
            Web sockets URI for the results

        label : str
            Optional string label to name the executor.
            Default: 'FuncXExecutor'

        auto_start : Bool
            Set this to start the result poller thread at init. If disabled the poller thread
            must be started by calling FuncXExecutor_object.start()
            Default: True
        """

        self.funcx_client = funcx_client
        self.results_ws_uri = results_ws_uri
        self.label = label
        self._tasks = {}
        self._function_registry = {}
        self._function_future_map = {}
        self._kill_event = threading.Event()
        self.task_group_id = str(uuid.uuid4())  # we need to associate all batch launches with this id
        if auto_start:
            self.start()

    def start(self):
        """ Start the result polling threads
        """

        # Currently we need to put the batch id's we launch into this queue
        # to tell the web_socket_poller to listen on them. Later we'll associate

        """
        self.task_group_queue = mp.Queue()
        self.thread = threading.Thread(target=self.web_socket_poller,
                                       args=(self._kill_event, self.task_group_queue, ))
        self.thread.start()
        """
        eventloop = asyncio.new_event_loop()
        self.eventloop = eventloop
        self.thread = threading.Thread(target=self.event_loop_thread,
                                       args=(eventloop, ))
        self.thread.start()
        self.poller_future = asyncio.run_coroutine_threadsafe(self.web_socket_poller(), eventloop)
        atexit.register(self.shutdown)
        logger.debug("Started web_socket_poller thread")

    def get_auth_header(self):
        """
        Gets an Authorization header to be sent during the WebSocket handshake. Based on
        header setting in the Globus SDK: https://github.com/globus/globus-sdk-python/blob/main/globus_sdk/base.py

        Returns
        -------
        Key-value tuple of the Authorization header
        [(key, value)]
        """
        headers = dict()
        self.funcx_client.authorizer.set_authorization_header(headers)
        header_name = 'Authorization'
        return (header_name, headers[header_name])

    def event_loop_thread(self, eventloop):
        asyncio.set_event_loop(eventloop)
        eventloop.run_forever()

    @asyncio.coroutine
    async def web_socket_poller(self):
        # print("[WSP]: In poller ")
        headers = self.get_auth_header()
        try:
            web_socket = await websockets.client.connect(self.results_ws_uri, extra_headers=[headers])
            self.ws = web_socket
            # initial Globus authentication happens during the HTTP portion of the handshake,
            # so an invalid handshake means that the user was not authenticated
        except InvalidHandshake:
            logger.exception("Connecting to the results web socket failed")
            raise Exception('Failed to authenticate user. Please ensure that you are logged in.')

        # print("[WSP]: connected ")
        # Send the task_group_id
        await web_socket.send(self.task_group_id)
        # print(f"[WSP] Sent task_group_id:{self.task_group_id} over ws")

        while True:
            # print("Waiting for data")
            raw_data = await web_socket.recv()
            # print(f"[WSP] Received raw_data : {type(raw_data)} : {raw_data}")
            data = json.loads(raw_data)
            task_id = data['task_id']

            if task_id in self._function_future_map:
                future = self._function_future_map[task_id]
                del self._function_future_map[task_id]
                try:
                    if data['result']:
                        future.set_result(self.funcx_client.fx_serializer.deserialize(data['result']))
                    elif data['exception']:
                        r_exception = self.funcx_client.fx_serializer.deserialize(data['exception'])
                        future.set_exception(dill.loads(r_exception.e_value))
                    else:
                        future.set_exception(Exception(data['reason']))
                except Exception as e:
                    print("Caught exception : ", e)
            else:
                print("[MISSING FUTURE]")

    def submit(self, function, *args, endpoint_id=None, container_uuid=None, **kwargs):
        """Initiate an invocation

        Parameters
        ----------
        function : Function/Callable
            Function / Callable to execute

        *args : Any
            Args as specified by the function signature

        endpoint_id : uuid str
            Endpoint UUID string. Required

        **kwargs : Any
            Arbitrary kwargs

        Returns
        -------
        Future : concurrent.futures.Future
            A future object
        """

        if function not in self._function_registry:
            # Please note that this is a partial implementation, not all function registration
            # options are fleshed out here.
            logger.debug("Function:{function} is not registered. Registering")
            function_uuid = self.funcx_client.register_function(function,
                                                                function_name=function.__name__,
                                                                container_uuid=container_uuid)
            self._function_registry[function] = function_uuid
            logger.debug(f"Function registered with id:{function_uuid}")
        assert endpoint_id is not None, "endpoint_id key-word argument must be set"

        batch = self.funcx_client.create_batch(batch_id=self.task_group_id)
        batch.add(*args,
                  endpoint_id=endpoint_id,
                  function_id=self._function_registry[function],
                  **kwargs)
        r = self.funcx_client.batch_run(batch)
        logger.debug(f"Batch submitted to task_group: {self.task_group_id}")

        task_uuid = r[0]
        # There's a potential for a race-condition here where the result reaches
        # the poller before the future is added to the future_map
        self._function_future_map[task_uuid] = Future()

        return self._function_future_map[task_uuid]

    def shutdown(self):
        if self._kill_event.is_set():
            return
        print("In shutdown")
        ws_close_future = asyncio.run_coroutine_threadsafe(self.ws.close(), self.eventloop)
        ws_close_future.result()
        self.poller_future.cancel()
        self.eventloop.call_soon_threadsafe(self.eventloop.stop)
        self.poller_future.cancel()
        self._kill_event.set()
        logger.debug(f"Executor:{self.label} shutting down")


def double(x):
    return x * 2


if __name__ == '__main__':

    import argparse
    from funcx import FuncXClient
    from funcx import set_stream_logger
    import time

    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--service_url", default='http://localhost:5000/v2',
                        help="URL at which the funcx-web-service is hosted")
    parser.add_argument("-e", "--endpoint_id", required=True,
                        help="Target endpoint to send functions to")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")
    args = parser.parse_args()

    endpoint_id = args.endpoint_id

    # set_stream_logger()
    fx = FuncXExecutor(FuncXClient(funcx_service_address=args.service_url))

    print("In main")
    endpoint_id = '766debbe-f01f-4b99-9f49-f806bcad99b5'
    future = fx.submit(double, 5, endpoint_id=endpoint_id)
    print("Got future back : ", future)

    for i in range(5):
        time.sleep(0.2)
        # Non-blocking check whether future is done
        print("Is the future done? :", future.done())

    print("Blocking for result")
    x = future.result()     # <--- This is a blocking call
    print("Result : ", x)

    fx.shutdown()
