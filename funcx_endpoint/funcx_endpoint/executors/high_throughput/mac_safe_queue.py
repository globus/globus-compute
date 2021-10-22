import platform

if platform.system() == "Darwin":
    from parsl.executors.high_throughput.mac_safe_queue import MacSafeQueue as mpQueue
else:
    from multiprocessing import Queue as mpQueue

__all__ = ("mpQueue",)
