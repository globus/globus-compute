import argparse
from funcx.serialize import FuncXSerializer
from funcx.queues import RedisQueue

def slow_double(i, duration=0):
    import time
    time.sleep(duration)
    return i*2

def test(endpoint_id=None, hostname=None, port=None):
    tasks_rq = RedisQueue(f'task_{endpoint_id}', hostname)
    results_rq = RedisQueue(f'results', hostname)
    fxs = FuncXSerializer()

    ser_code = fxs.serialize(slow_double)
    fn_code = fxs.pack_buffers([ser_code])

    tasks_rq.connect()
    results_rq.connect()
    for i in range(10):
        ser_args = fxs.serialize([i])
        ser_kwargs = fxs.serialize({'duration':0})
        input_data = fxs.pack_buffers([ser_args, ser_kwargs])
        payload = fn_code + input_data
        tasks_rq.put(f"0{i}", payload)

    for i in range(10):
        res = results_rq.get(timeout=1)
        print("Result : ", res)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-r", "--redis_hostname", required=True,
                        help="Hostname of the Redis server")
    parser.add_argument("-e", "--endpoint_id", required=True,
                        help="Endpoint_id")
    args = parser.parse_args()


    test(endpoint_id=args.endpoint_id,
         hostname=args.redis_hostname)
