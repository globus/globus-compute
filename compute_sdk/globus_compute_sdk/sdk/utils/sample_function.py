def sdk_tutorial_sample_function():
    """
    Simple echo function in its own module for diagnostic tests
    """
    from datetime import datetime as dt
    from datetime import timezone

    return f"Hello world at {dt.now(timezone.utc).isoformat()}"
