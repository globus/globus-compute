import json
import sys
import argparse
import time
import funcx
from funcx import FuncXClient

def sum_yadu_new01(event):
    return sum(event)

"""
@funcx.register(description="...")
def sum_yadu_new01(event):
    return sum(event)
"""

def test(fxc, ep_id):

    fn_uuid = fxc.register_function(sum_yadu_new01,
                                    description="New sum function defined without string spec")
    print("FN_UUID : ", fn_uuid)


    task_id = fxc.run([1,2,3,9001], endpoint_id=ep_id, function_id=fn_uuid)
    r = fxc.get_task_status(task_id)
    print("Got from status :", r)

def platinfo():
    import platform
    return platform.uname()

def test2(fxc, ep_id):

    fn_uuid = fxc.register_function(platinfo,
                                    description="Get platform info")
    print("FN_UUID : ", fn_uuid)
    task_id = fxc.run(endpoint_id=ep_id, function_id=fn_uuid)

    time.sleep(1)
    r = fxc.get_task_status(task_id)
    print("Got from status :", r)



if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-e", "--endpoint", required=True)
    args = parser.parse_args()

    print("FuncX version : ", funcx.__version__)
    fxc = FuncXClient()
    #test(fxc, args.endpoint)
    test2(fxc, args.endpoint)

