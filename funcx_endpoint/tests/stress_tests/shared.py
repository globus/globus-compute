def simple_function(*, input_data: bytes, output_size: int, sleep_dur: int):
    import time

    time.sleep(sleep_dur)
    return (len(input_data), bytearray(output_size))


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
