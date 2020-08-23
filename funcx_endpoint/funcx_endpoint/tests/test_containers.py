import json
import sys
import argparse
import time
import funcx
from funcx.sdk.client import FuncXClient


def container_sum(event):
    return sum(event)


def test(fxc, ep_id):

    fn_uuid = fxc.register_function(container_sum,
                                    container_uuid='3861862b-152e-49a4-b15e-9a5da4205cad',
                                    description="New sum function defined without string spec")
    print("FN_UUID : ", fn_uuid)

    task_id = fxc.run([1, 2, 3, 9001], endpoint_id=ep_id, function_id=fn_uuid)
    r = fxc.get_task_status(task_id)
    print("Got from status :", r)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-e", "--endpoint", required=True)
    args = parser.parse_args()

    print("FuncX version : ", funcx.__version__)
    fxc = FuncXClient()
    test(fxc, args.endpoint)
