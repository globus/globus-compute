def sdk_tutorial_sample_simple_function():
    """
    Simplest possible function to execute on the endpoint in terms of serialization
    """
    return "Hello world from Globus Compute Self Diagnostic"


def sdk_tutorial_sample_function():
    """
    Simple echo function in its own module for diagnostic tests

    Note that this function still may not deserialize with some
    serialization methods such as DillCode due to module import issues

    This sample function includes some import statements and is a more complex
    version of the "simple" version above
    """
    from datetime import datetime as dt
    from datetime import timezone

    return f"Hello world at {dt.now(timezone.utc).isoformat()}"
