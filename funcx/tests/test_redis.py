import argparse
from funcx.serialize import FuncXSerializer
from funcx.queues import RedisQueue
import time

def slow_double(i, duration=0):
    import time
    time.sleep(duration)
    return i*2

def test(endpoint_id=None, tasks=10, hostname=None, port=None):
    tasks_rq = RedisQueue(f'task_{endpoint_id}', hostname)
    results_rq = RedisQueue(f'results', hostname)
    fxs = FuncXSerializer()

    ser_code = fxs.serialize(slow_double)
    fn_code = fxs.pack_buffers([ser_code])

    tasks_rq.connect()
    results_rq.connect()

    while True:
        try:
            x = results_rq.get(timeout=1)
        except:
            print("No more results left")
            break

    start = time.time()
    for i in range(tasks):
        ser_args = fxs.serialize([i])
        ser_kwargs = fxs.serialize({'duration':0})
        input_data = fxs.pack_buffers([ser_args, ser_kwargs])
        payload = fn_code + input_data
        container_id = "odd" if i%2 else "even"
        tasks_rq.put(f"0{i};{container_id}", payload)

    print(f"Launched {tasks} tasks")
    for i in range(tasks):
        res = results_rq.get(timeout=10)
        print("Result : ", res)

    delta = time.time() - start
    print("Time to complete {} tasks: {:8.3f} s".format(tasks, delta))
    print("Throughput : {:8.3f} Tasks/s".format(tasks / delta))
    return delta


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-r", "--redis_hostname", required=True,
                        help="Hostname of the Redis server")
    parser.add_argument("-e", "--endpoint_id", required=True,
                        help="Endpoint_id")
    parser.add_argument("-c", "--count", required=True,
                        help="Number of tasks")

    args = parser.parse_args()


    test(endpoint_id=args.endpoint_id,
         hostname=args.redis_hostname,
         tasks=int(args.count))
