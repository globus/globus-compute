import time

import numpy
import pytest


def dummy_fn(*args, **kwargs):
    return (args, kwargs)


test_cases = [
    1,  # int
    1.0,  # float
    "Hello",  # str
    numpy.random.rand(3, 2),  # small numpy array < 4kb
    numpy.random.rand(100, 100),  # larger numpy array, ~80Kb
    ["list", "of", "objects"],
    {"dict": "of", "random": "object"},
]


@pytest.mark.parametrize("param", test_cases)
def test_params(fxc, endpoint, param):
    fn_uuid = fxc.register_function(dummy_fn, endpoint, description="arb_fn")

    task_id = fxc.run(param, endpoint_id=endpoint, function_id=fn_uuid)

    flag = False
    for _i in range(5):
        try:
            r = fxc.get_result(task_id)
            print(f"result : {r}")

        # This is pretty terrible, until we fix the exception to differentiate TypeError
        except Exception:
            time.sleep(2)
        else:
            flag = True
            break

    assert flag, "Task failed to return in 5s"
