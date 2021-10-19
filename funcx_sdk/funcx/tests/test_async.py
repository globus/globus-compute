import asyncio
import random


def squared(x):
    return x ** 2


def delay_n(n):
    import time

    time.sleep(n)
    return "hello"


async def simple_task(fxc, endpoint):
    squared_function = fxc.register_function(squared)
    x = random.randint(0, 100)
    task = fxc.run(x, endpoint_id=endpoint, function_id=squared_function)
    result = await asyncio.wait_for(task, 10)
    assert result == squared(x), "Got wrong answer"


async def batch_task(fxc, endpoint):
    expected_results = []
    squared_function = fxc.register_function(squared)
    inputs = list(range(10))
    batch = fxc.create_batch()
    for x in inputs:
        batch.add(x, endpoint_id=endpoint, function_id=squared_function)
        expected_results.append(squared(x))
    batch_res = fxc.batch_run(batch)

    await asyncio.sleep(10)

    for i in range(len(batch_res)):
        task = batch_res[i]
        expected_result = expected_results[i]
        result = await asyncio.wait_for(task, 1)
        assert result == expected_result, "Got wrong answer"


async def submit_while_waiting_task(fxc, endpoint):
    squared_function = fxc.register_function(squared)
    delay_n_function = fxc.register_function(delay_n)

    task1 = fxc.run(3, endpoint_id=endpoint, function_id=delay_n_function)

    x = random.randint(0, 100)
    task2 = fxc.run(x, endpoint_id=endpoint, function_id=squared_function)
    result2 = await asyncio.wait_for(task2, 10)
    assert result2 == squared(x), "Got wrong answer"
    result1 = await asyncio.wait_for(task1, 10)
    assert result1 == "hello", "Got wrong answer"


def test_simple(async_fxc, endpoint):
    async_fxc.loop.run_until_complete(simple_task(async_fxc, endpoint))


def test_batch(async_fxc, endpoint):
    async_fxc.loop.run_until_complete(batch_task(async_fxc, endpoint))


def test_submit_while_waiting(async_fxc, endpoint):
    async_fxc.loop.run_until_complete(submit_while_waiting_task(async_fxc, endpoint))
