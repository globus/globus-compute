class CouldNotExecuteUserTaskError(Exception):
    """
    generic exception class for errors while attempting to setup and run the user
    function but which does not come from the user function itself failing
    """
