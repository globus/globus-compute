import zmq
import pickle as pkl


class ZmqClient(object):
    """
    ZMQ client. Connect to the funcX service to receive jobs.
    """

    def __init__(self, ip_address="*", port=5555):
        print ("Setting up listener")
        context = zmq.Context()
        self.server = context.socket(zmq.REP)
        print("connecting to server on {}:{}".format(ip_address, port))
        self.server.connect("tcp://{}:{}".format(ip_address, port))

    def send(self, msg):
        return self.server.send(msg)

    def recv(self):
        return self.server.recv()


