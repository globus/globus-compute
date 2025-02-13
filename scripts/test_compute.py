import os
import sys
import time
import uuid

from globus_compute_sdk import Client, Executor
from globus_compute_sdk.errors.error_types import TaskPending

"""
Usage:  python3 test_compute.py <EP_UUID> [env]
  where env is optional, defaults to test, and can be one of ENVS below
"""


def hello_test(s):
    return f"Hello {s}"


ENVS = ["production", "test", "preview", "integration", "sandbox"]

if __name__ == "__main__":
    assert len(sys.argv) > 1, "Please provide the Endpoint UUID as argument!"
    ep_id = sys.argv[1]
    assert uuid.UUID(ep_id), f"Invalid UUID: {ep_id}"
    arg_uuid = str(uuid.uuid4())

    env = ENVS[1]
    if len(sys.argv) > 2:
        env = sys.argv[2].lower()
        assert env in ENVS, f"Supported environments: {ENVS}"

    os.environ["GLOBUS_SDK_ENVIRONMENT"] = env
    gcc = Client()
    fid = gcc.register_function(hello_test)
    tid = gcc.run(arg_uuid, endpoint_id=ep_id, function_id=fid)
    print(f"Registered function {fid} on {env}, waiting for results...")
    res = None
    wait_times = [5, 5, 5, 5]
    for sleep_s in wait_times:
        try:
            time.sleep(sleep_s)
            res = gcc.get_result(tid)
        except TaskPending:
            continue
    assert (
        res
    ), "Failed to get result using Globus Compute Client after {sum(wait_times)} s"
    assert arg_uuid in res, f"Invalid result: {res}"
    gce = Executor(endpoint_id=ep_id)
    fut = gce.submit_to_registered_function(fid, args=[arg_uuid])
    assert arg_uuid in fut.result()
    print(f"Test success on Endpoint {ep_id} with task {tid} via Client and Executor!")
