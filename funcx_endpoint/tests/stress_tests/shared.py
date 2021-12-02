def simple_function(*, input_data: bytes, output_size: int, sleep_dur: int):
    import time

    time.sleep(sleep_dur)
    return (len(input_data), bytearray(output_size))
