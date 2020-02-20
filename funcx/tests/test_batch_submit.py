import json
import sys
import argparse
import time
import funcx
from funcx import FuncXClient
from funcx.serialize import FuncXSerializer
fxs = FuncXSerializer()

# funcx.set_stream_logger()

def double(x):
    return x * 2


def test(fxc, ep_id):

    fn_uuid = fxc.register_function(double,
                                    description="Yadu double")
    print("FN_UUID : ", fn_uuid)

    task_ids = fxc.map_run([1,2,3,9001], endpoint_id=ep_id, function_id=fn_uuid)

    # task_ids = [fxc.run(i, endpoint_id=ep_id, function_id=fn_uuid) for i in range(5)]
    print("Got tasks_ids : ", task_ids)

    for i in range(3):
        print("Batch status : ", fxc.get_batch_status(task_ids))
        time.sleep(1)

    """
    for task_id in task_ids:
        r = fxc.get_task_status(task_id)
        print("Got from status :", r)
    """

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-e", "--endpoint", required=True)
    args = parser.parse_args()

    print("FuncX version : ", funcx.__version__)
    fxc = FuncXClient(funcx_service_address='https://dev.funcx.org/api/v1')
    test(fxc, args.endpoint)

