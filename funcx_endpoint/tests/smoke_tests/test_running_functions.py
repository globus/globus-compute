import time


def test_run_pre_registered_function(fxc, tutorial_endpoint, tutorial_funcion_id):
    """This test confirms that we are connected to the default production DB"""
    fn_id = fxc.run(endpoint_id=tutorial_endpoint, function_id=tutorial_funcion_id)

    time.sleep(10)

    result = fxc.get_result(fn_id)
    assert result == "Hello World!", f"Expected result: Hello World!, got {result}"


def double(x):
    return x * 2


def test_batch(fxc, try_tutorial_endpoint):
    """Test batch submission and get_batch_result"""

    double_fn = fxc.register_function(double)

    inputs = list(range(10))
    batch = fxc.create_batch()

    for x in inputs:
        batch.add(x, endpoint_id=try_tutorial_endpoint, function_id=double_fn)

    batch_res = fxc.batch_run(batch)

    time.sleep(10)

    results = fxc.get_batch_result(batch_res)

    total = sum(results[tid]["result"] for tid in results)
    assert total == 2 * (sum(inputs)), "Batch run results do not add up"
