import logging
import os
import uuid

import zmq
import zmq.auth
from zmq.auth.thread import ThreadAuthenticator

log = logging.getLogger(__name__)


class TaskQueue:
    """Outgoing task queue from the executor to the Interchange"""

    def __init__(
        self,
        address: str,
        port: int = 55001,
        identity: str = str(uuid.uuid4()),
        zmq_context=None,
        set_hwm=False,
        RCVTIMEO=None,
        SNDTIMEO=None,
        linger=None,
        ironhouse: bool = False,
        keys_dir: str = os.path.abspath(".curve"),
        mode: str = "client",
    ):
        """
        Parameters
        ----------

        address: str
           address to connect

        port: int
           Port to use

        identity : str
           Applies only to clients, where the identity must match the endpoint uuid.
           This will be utf-8 encoded on the wire. A random uuid4 string is set by
           default.

        mode: string
           Either 'client' or 'server'

        keys_dir : string
           Directory from which keys will be loaded for curve.

        ironhouse: Bool
           Only valid for server mode. Setting this flag switches the server to require
           client keys to be available on the server in the keys_dir.
        """
        if zmq_context:
            self.context = zmq_context
        else:
            self.context = zmq.Context()

        self.mode = mode
        self.port = port
        self.ironhouse = ironhouse
        self.keys_dir = keys_dir

        assert self.mode in [
            "client",
            "server",
        ], "Only two modes are supported: client, server"

        if self.mode == "server":
            print("Configuring server")
            self.zmq_socket = self.context.socket(zmq.ROUTER)
            self.zmq_socket.set(zmq.ROUTER_MANDATORY, 1)
            self.zmq_socket.set(zmq.ROUTER_HANDOVER, 1)
            print("Setting up auth-server")
            self.setup_server_auth()
        elif self.mode == "client":
            self.zmq_socket = self.context.socket(zmq.DEALER)
            self.setup_client_auth()
            self.zmq_socket.setsockopt(zmq.IDENTITY, identity.encode("utf-8"))
        else:
            raise ValueError(
                "TaskQueue must be initialized with mode set to 'server' or 'client'"
            )

        if set_hwm:
            self.zmq_socket.set_hwm(0)
        if RCVTIMEO is not None:
            self.zmq_socket.setsockopt(zmq.RCVTIMEO, RCVTIMEO)
        if SNDTIMEO is not None:
            self.zmq_socket.setsockopt(zmq.SNDTIMEO, SNDTIMEO)
        if linger is not None:
            self.zmq_socket.setsockopt(zmq.LINGER, linger)

        # all zmq setsockopt calls must be done before bind/connect is called
        if self.mode == "server":
            self.zmq_socket.bind(f"tcp://*:{port}")
        elif self.mode == "client":
            self.zmq_socket.connect(f"tcp://{address}:{port}")

        self.poller = zmq.Poller()
        self.poller.register(self.zmq_socket)
        os.makedirs(self.keys_dir, exist_ok=True)
        log.debug(f"Initializing Taskqueue:{self.mode} on port:{self.port}")

    def zmq_context(self):
        return self.context

    def add_client_key(self, endpoint_id, client_key):
        log.info("Adding client key")
        if self.ironhouse:
            # Use the ironhouse ZMQ pattern: http://hintjens.com/blog:49#toc6
            with open(os.path.join(self.keys_dir, f"{endpoint_id}.key"), "w") as f:
                f.write(client_key)
            try:
                self.auth.configure_curve(domain="*", location=self.keys_dir)
            except Exception:
                log.exception("Failed to load keys from {self.keys_dir}")
        return

    def setup_server_auth(self):
        # Start an authenticator for this context.
        self.auth = ThreadAuthenticator(self.context)
        self.auth.start()
        self.auth.allow("127.0.0.1")
        # Tell the authenticator how to handle CURVE requests

        if not self.ironhouse:
            # Use the stonehouse ZMQ pattern: http://hintjens.com/blog:49#toc5
            self.auth.configure_curve(domain="*", location=zmq.auth.CURVE_ALLOW_ANY)

        server_secret_file = os.path.join(self.keys_dir, "server.key_secret")
        server_public, server_secret = zmq.auth.load_certificate(server_secret_file)
        self.zmq_socket.curve_secretkey = server_secret
        self.zmq_socket.curve_publickey = server_public
        self.zmq_socket.curve_server = True  # must come before bind

    def setup_client_auth(self):
        # We need two certificates, one for the client and one for
        # the server. The client must know the server's public key
        # to make a CURVE connection.
        client_secret_file = os.path.join(self.keys_dir, "endpoint.key_secret")
        client_public, client_secret = zmq.auth.load_certificate(client_secret_file)
        self.zmq_socket.curve_secretkey = client_secret
        self.zmq_socket.curve_publickey = client_public

        # The client must know the server's public key to make a CURVE connection.
        server_public_file = os.path.join(self.keys_dir, "server.key")
        server_public, _ = zmq.auth.load_certificate(server_public_file)
        self.zmq_socket.curve_serverkey = server_public

    def get(self, block=True, timeout=1000):
        """
        Parameters
        ----------

        block : Bool
            Blocks until there's a message, Default is True
        timeout : int
            Milliseconds to wait.
        """
        # timeout is in milliseconds
        if block is True:
            return self.zmq_socket.recv_multipart()

        socks = dict(self.poller.poll(timeout=timeout))
        if self.zmq_socket in socks and socks[self.zmq_socket] == zmq.POLLIN:
            message = self.zmq_socket.recv_multipart()
            return message
        else:
            raise zmq.Again

    def register_client(self, message):
        return self.zmq_socket.send_multipart([message])

    def put(self, dest, message, max_timeout=1000):
        """This function needs to be fast at the same time aware of the possibility of
        ZMQ pipes overflowing.

        The timeout increases slowly if contention is detected on ZMQ pipes.
        We could set copy=False and get slightly better latency but this results
        in ZMQ sockets reaching a broken state once there are ~10k tasks in flight.
        This issue can be magnified if each the serialized buffer itself is larger.

        Parameters
        ----------

        dest : zmq_identity of the destination endpoint, must be a byte string

        message : py object
             Python object to send

        max_timeout : int
             Max timeout in milliseconds that we will wait for before raising an
             exception

        Raises
        ------

        zmq.EAGAIN if the send failed.
        zmq.error.ZMQError: Host unreachable (if client disconnects?)

        """
        if self.mode == "client":
            return self.zmq_socket.send_multipart([message])
        else:
            return self.zmq_socket.send_multipart([dest, message])

    def close(self):
        self.zmq_socket.close()
        self.context.term()
