import pickle
import time
import uuid
from multiprocessing import Queue

from funcx.serialize import FuncXSerializer
from funcx_endpoint.executors.high_throughput.executor import HighThroughputExecutor
from funcx_endpoint.executors.high_throughput.messages import EPStatusReport, Task


def double(x):
    return x * 2


def test_passthrough():
    results_queue = Queue()
    htex = HighThroughputExecutor(
        interchange_local=True,
        passthrough=True,
        run_dir=".",
        endpoint_id=str(uuid.uuid4()),
    )

    htex.start(results_passthrough=results_queue)
    fx_serializer = FuncXSerializer()

    count = 10
    task_ids = {}
    for i in range(count):
        task_id = str(uuid.uuid4())
        args = (i,)
        kwargs = {}

        fn_code = fx_serializer.serialize(double)
        ser_code = fx_serializer.pack_buffers([fn_code])
        ser_params = fx_serializer.pack_buffers(
            [fx_serializer.serialize(args), fx_serializer.serialize(kwargs)]
        )

        payload = Task(task_id, "RAW", ser_code + ser_params)
        htex.submit_raw(payload.pack())
        time.sleep(0.5)
        task_ids[task_id] = None

    while count:
        result_package = results_queue.get(timeout=60)
        print("Got :", result_package)
        assert "message" in result_package
        ret_obj = pickle.loads(result_package["message"])
        print(ret_obj)

        if result_package["task_id"] is None:
            # we got a EPStatusReport
            assert isinstance(ret_obj, EPStatusReport)
        else:
            assert result_package["task_id"] in task_ids
            count -= 1

    print("All done")
    htex.shutdown()


if __name__ == "__main__":
    test_passthrough()
