class CouldNotExecuteUserTaskError(Exception):
    """
    generic exception class for errors while attempting to setup and run the user
    function but which does not come from the user function itself failing
    """


class MessageSystemExit(SystemExit):
    def __init__(self, code: int | str = 0, msg: str = ""):
        self.msg = msg
        super().__init__(code)

    def __str__(self) -> str:
        return self.msg
