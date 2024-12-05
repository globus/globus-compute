#!/usr/bin/env python3

from __future__ import annotations

import ipaddress
import logging
import time

import dill
import zmq
from globus_compute_endpoint.engines.high_throughput.messages import Message

log = logging.getLogger(__name__)


def _zmq_canonicalize_address(addr: str | int) -> str:
    try:
        ip = ipaddress.ip_address(addr)
    except ValueError:
        # Not a valid IPv4 or IPv6 address
        if isinstance(addr, int):
            # If it was an integer, then it's just plain invalid
            raise

        # Otherwise, it was likely a hostname; let another layer deal with it
        return addr

    if ip.version == 4:
        return str(ip)  # like "12.34.56.78"
    elif ip.version == 6:
        return f"[{ip}]"  # like "[::1]"


def _zmq_create_socket_port(context: zmq.Context, ip_address: str | int, port_range):
    """
    Utility method with logic shared by all the pipes
    """
    sock = context.socket(zmq.DEALER)
    sock.set_hwm(0)
    # This option should work for both IPv4 and IPv6...?
    # May not work until Parsl is updated?
    sock.setsockopt(zmq.IPV6, True)

    port = sock.bind_to_random_port(
        f"tcp://{_zmq_canonicalize_address(ip_address)}",
        min_port=port_range[0],
        max_port=port_range[1],
    )
    return sock, port


class CommandClient:
    """CommandClient"""

    def __init__(self, ip_address, port_range):
        """
        Parameters
        ----------

        ip_address: str
           IP address of the client (where Parsl runs)
        port_range: tuple(int, int)
           Port range for the comms between client and interchange

        """

        self.context = zmq.Context()
        self.zmq_socket, self.port = _zmq_create_socket_port(
            self.context, ip_address, port_range
        )

    def run(self, message):
        """This function needs to be fast at the same time aware of the possibility of
        ZMQ pipes overflowing.

        The timeout increases slowly if contention is detected on ZMQ pipes.
        We could set copy=False and get slightly better latency but this results
        in ZMQ sockets reaching a broken state once there are ~10k tasks in flight.
        This issue can be magnified if each the serialized buffer itself is larger.
        """
        self.zmq_socket.send(message.pack(), copy=True)
        reply = self.zmq_socket.recv()
        return Message.unpack(reply)

    def close(self):
        self.zmq_socket.close()
        self.context.term()


class TasksOutgoing:
    """Outgoing task queue from the Engine to the Interchange"""

    def __init__(self, ip_address, port_range):
        """
        Parameters
        ----------

        ip_address: str
           IP address of the client (where Parsl runs)
        port_range: tuple(int, int)
           Port range for the comms between client and interchange

        """
        self.context = zmq.Context()
        self.zmq_socket, self.port = _zmq_create_socket_port(
            self.context, ip_address, port_range
        )
        self.poller = zmq.Poller()
        self.poller.register(self.zmq_socket, zmq.POLLOUT)

    def put(self, message, max_timeout=1000):
        """This function needs to be fast at the same time aware of the possibility of
        ZMQ pipes overflowing.

        The timeout increases slowly if contention is detected on ZMQ pipes.
        We could set copy=False and get slightly better latency but this results
        in ZMQ sockets reaching a broken state once there are ~10k tasks in flight.
        This issue can be magnified if each the serialized buffer itself is larger.

        Parameters
        ----------

        message : py object
             Python object to send
        max_timeout : int
             Max timeout in milliseconds that we will wait for before raising an
             exception

        Raises
        ------

        zmq.EAGAIN if the send failed.

        """
        timeout_ms = 0
        current_wait = 0
        while current_wait < max_timeout:
            socks = dict(self.poller.poll(timeout=timeout_ms))
            if self.zmq_socket in socks and socks[self.zmq_socket] == zmq.POLLOUT:
                # The copy option adds latency but reduces the risk of ZMQ overflow
                self.zmq_socket.send(message, copy=True)
                return
            else:
                timeout_ms += 1
                log.debug(
                    "Not sending due to full zmq pipe, timeout: {} ms".format(
                        timeout_ms
                    )
                )
            current_wait += timeout_ms

        # Send has failed.
        log.debug(f"Remote side has been unresponsive for {current_wait}")
        raise zmq.error.Again

    def close(self):
        self.zmq_socket.close()
        self.context.term()


class ResultsIncoming:
    """Incoming results queue from the Interchange to the Engine"""

    def __init__(self, ip_address, port_range):
        """
        Parameters
        ----------

        ip_address: str
           IP address of the client (where Parsl runs)
        port_range: tuple(int, int)
           Port range for the comms between client and interchange

        """
        self.context = zmq.Context()
        self.results_receiver, self.port = _zmq_create_socket_port(
            self.context, ip_address, port_range
        )

    def get(self, block=True, timeout=None):
        block_messages = self.results_receiver.recv()
        try:
            res = dill.loads(block_messages)
        except dill.UnpicklingError:
            try:
                res = Message.unpack(block_messages)
            except Exception:
                log.exception(
                    "Message in results queue is not pickle/Message formatted: %s",
                    block_messages,
                )
        return res

    def request_close(self):
        status = self.results_receiver.send(dill.dumps(None))
        time.sleep(0.1)
        return status

    def close(self):
        self.results_receiver.close()
        self.context.term()
