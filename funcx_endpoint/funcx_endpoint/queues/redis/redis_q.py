import redis
import json

from funcx_endpoint.queues.base import NotConnected, FuncxQueue


class RedisQueue(FuncxQueue):
    """ A basic redis queue

    The queue only connects when the `connect` method is called to avoid
    issues with passing an object across processes.

    Parameters
    ----------

    hostname : str
       Hostname of the redis server

    port : int
       Port at which the redis server can be reached. Default: 6379

    """

    def __init__(self, prefix, hostname, port=6379):
        """ Initialize
        """
        self.hostname = hostname
        self.port = port
        self.redis_client = None
        self.prefix = prefix

    def connect(self):
        """ Connects to the Redis server
        """
        try:
            if not self.redis_client:
                self.redis_client = redis.StrictRedis(host=self.hostname, port=self.port, decode_responses=True)
        except redis.exceptions.ConnectionError:
            print("ConnectionError while trying to connect to Redis@{}:{}".format(self.hostname,
                                                                                  self.port))

            raise

    def get(self, timeout=1):
        """ Get an item from the redis queue

        Parameters
        ----------
        timeout : int
           Timeout for the blocking get in seconds
        """
        try:
            task_list, task_id = self.redis_client.blpop(f'{self.prefix}_list', timeout=timeout)
            jtask_info = self.redis_client.get(f'{self.prefix}:{task_id}')
            task_info = json.loads(jtask_info)
        except AttributeError:
            raise NotConnected(self)
        except redis.exceptions.ConnectionError:
            print(f"ConnectionError while trying to connect to Redis@{self.hostname}:{self.port}")
            raise

        return task_id, task_info

    def put(self, key, payload):
        """ Put's the key:payload into a dict and pushes the key onto a queue
        Parameters
        ----------
        key : str
            The task_id to be pushed

        payload : dict
            Dict of task information to be stored
        """
        try:
            self.redis_client.set(f'{self.prefix}:{key}', json.dumps(payload))
            self.redis_client.rpush(f'{self.prefix}_list', key)
        except AttributeError:
            raise NotConnected(self)
        except redis.exceptions.ConnectionError:
            print("ConnectionError while trying to connect to Redis@{}:{}".format(self.hostname,
                                                                                  self.port))
            raise

    @property
    def is_connected(self):
        return self.redis_client is not None


def test():
    rq = RedisQueue('task', '127.0.0.1')
    rq.connect()
    rq.put("01", {'a': 1, 'b': 2})
    res = rq.get(timeout=1)
    print("Result : ", res)


if __name__ == '__main__':
    test()
