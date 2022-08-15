from multiprocessing import Process


class MacProcess(Process):
    """
    Start Process in start() instead of in __init__ to avoid possible
    IOLoop issue before a fork and trying to use it in other processes.
    Key is doing it before anything creates the singleton IOLoop?
    """

    def __init__(self, args=None, target=None, name=None, **kwargs):
        self.target = target
        self.name = name
        self.comm_q = args[0]
        self.kwargs = kwargs
        self._process = None
        super().__init__(**self.kwargs)

    def start(self):
        self._process = Process(
            args=(self.comm_q,), target=self.target, name=self.name, **self.kwargs
        )
        return self._process.start()
