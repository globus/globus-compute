from __future__ import annotations

import time

from globus_compute_sdk import Client, Executor
from globus_compute_sdk.sdk._environments import TUTORIAL_EP_UUID
from globus_compute_sdk.sdk.utils.sample_function import sdk_tutorial_sample_function

# No arguments, just returns "Hello World" + UTC time, encoded in PY 3.11.8
ECHO_TIME_FUID = "4e7b5079-6793-4f88-ab64-ae223377a35f"
# Potentially use other functions for testing
EXISTING_FUID = ECHO_TIME_FUID


class TutorialEndpointTester:
    def __init__(self, ep_uuid: str | None = None):
        self.gcc = Client()
        self.fn_uuid = None
        self.ep_uuid = ep_uuid if ep_uuid else TUTORIAL_EP_UUID

    def register_sample_function(self) -> str:
        self.fn_uuid = self.gcc.register_function(sdk_tutorial_sample_function)
        return self.fn_uuid

    def run_func_with_client(self, max_wait_s: int = 30, fuid=None):
        resp = self.gcc.run(endpoint_id=self.ep_uuid, function_id=fuid)
        e_msg = None

        # Check for results until the time limit allowed
        while max_wait_s >= 0:
            time.sleep(5)
            try:
                print(self.gcc.get_result(resp))
                return
            except Exception as e:
                e_msg = f"{str(e)}"
                if "pending due to" not in e_msg:
                    print(self.unexpected_error("Client", e))
            max_wait_s -= 5
        if e_msg is not None:
            print(e_msg)
        else:
            print(f"No result from Client task submission to {self.ep_uuid}")

    def run_existing_func_with_executor(self):
        with Executor(funcx_client=self.gcc, endpoint_id=self.ep_uuid) as gce:
            try:
                fut = gce.submit_to_registered_function(EXISTING_FUID)
                print(fut.result(timeout=30))
            except Exception as e:
                print(self.unexpected_error("Executor", e))

    def run_new_func_with_executor(self):
        with Executor(funcx_client=self.gcc, endpoint_id=self.ep_uuid) as gce:
            try:
                print(f"Sending a task to {self.ep_uuid}...")
                fut = gce.submit(sdk_tutorial_sample_function)
                print(fut.result(timeout=20))
            except Exception as e:
                print(self.unexpected_error("Executor", e))

    @staticmethod
    def unexpected_error(method: str, e: Exception):
        return (
            f"Unexpected error submitting to Globus Compute {method}:\n"
            f"  {type(e)}\n{e}"
        )

    def run_existing_func_with_Client(self):
        self.run_func_with_client(fuid=EXISTING_FUID)

    def run_new_func_with_Client(self):
        if self.fn_uuid is None:
            self.register_sample_function()
        self.run_func_with_client(fuid=self.fn_uuid)
