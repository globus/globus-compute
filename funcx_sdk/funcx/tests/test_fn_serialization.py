import time

from funcx.tests.fn_module import imported_fn
from funcx.utils.errors import TaskPending


def local_fn():
    return True


def test_imported_function(fxc, endpoint):

    fn_uuid = fxc.register_function(imported_fn, endpoint, description="platinfo")
    task_id = fxc.run(endpoint_id=endpoint, function_id=fn_uuid)

    print("Task_id: ", task_id)

    for _i in range(5):
        try:
            r = fxc.get_result(task_id)
            print(f"result : {r}")
        except TaskPending:
            time.sleep(2)
        else:
            break


def test_local_function(fxc, endpoint):

    fn_uuid = fxc.register_function(local_fn, endpoint, description="local_fn")
    task_id = fxc.run(endpoint_id=endpoint, function_id=fn_uuid)

    print("Task_id: ", task_id)

    for _i in range(5):
        try:
            r = fxc.get_result(task_id)
            print(f"result : {r}")
        except TaskPending:
            time.sleep(2)
        else:
            break


def test_nested_scope_function(fxc, endpoint):
    def in_scope_fn():
        return False

    fn_uuid = fxc.register_function(in_scope_fn, endpoint, description="in_scope_fn")
    task_id = fxc.run(endpoint_id=endpoint, function_id=fn_uuid)

    print("Task_id: ", task_id)

    for _i in range(5):
        try:
            r = fxc.get_result(task_id)
            print(f"result : {r}")
        except TaskPending:
            time.sleep(2)
        else:
            break


def increment_decorator(func):
    def wrapper(*args, **kwargs):
        x = func(*args, **kwargs)
        return x + 1

    return wrapper


@increment_decorator
def double(x):
    return x * 2


def test_decorated_function(fxc, endpoint):

    x = 42
    fn_uuid = fxc.register_function(double, endpoint, description="platinfo")
    task_id = fxc.run(x, endpoint_id=endpoint, function_id=fn_uuid)

    print("Task_id: ", task_id)

    time.sleep(10)
    r = fxc.get_result(task_id)
    assert r == double(x), "Incorrect result returned"
