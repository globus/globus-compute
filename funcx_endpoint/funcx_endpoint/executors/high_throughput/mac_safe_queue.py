import platform

if platform.system() == "Darwin":
    from parsl.multiprocessing import MacSafeQueue as mpQueue
else:
    from multiprocessing import Queue as mpQueue

__all__ = ("mpQueue",)
