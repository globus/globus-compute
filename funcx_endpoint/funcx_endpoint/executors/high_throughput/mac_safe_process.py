import platform
if platform.system() == 'Darwin':
    from parsl.executors.high_throughput.mac_safe_process import MacSafeProcess as mpProcess
else:
    from multiprocessing import Process as mpProcess
