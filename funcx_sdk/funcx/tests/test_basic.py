import time

from funcx.sdk.client import FuncXClient


def hello_world() -> str:
    return "Hello World"


def wait_for_task(fxc, task_id, walltime: int = 2):
    import time

    start = time.time()
    while True:
        if time.time() > start + walltime:
            raise Exception("Timeout")
        try:
            r = fxc.get_result(task_id)
        except Exception:
            print("Not available yet")
            time.sleep(1)
        else:
            return r


def test_blocking(fxc, endpoint):
    fn_uuid = fxc.register_function(hello_world, endpoint, description="Hello")
    task_id = fxc.run(endpoint_id=endpoint, function_id=fn_uuid)

    print("Task_id: ", task_id)
    time.sleep(2)
    r = fxc.get_result(task_id)
    print("Task_result : ", r)


def test_non_blocking(fxc, endpoint):
    fn_uuid = fxc.register_function(hello_world, endpoint, description="Hello")
    task_id = fxc.run(endpoint_id=endpoint, function_id=fn_uuid)

    for _i in range(5):
        try:
            result = fxc.get_task_status(task_id)
        except Exception as e:
            print(f"Got exception : {e}")
            time.sleep(1)
        else:
            print(f"Result: {result}")
            assert result == hello_world(), "Result from remote function not correct"
            break


if __name__ == "__main__":

    fxc = FuncXClient(funcx_service_address="http://k8s-dev.funcx.org/api/v1")
    endpoint = "968cdce1-f87b-4bda-9c99-3ed332ee3df2"
    test_blocking(fxc, endpoint)
