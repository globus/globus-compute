import traceback
from functools import lru_cache

import pytest
from globus_compute_sdk.errors import TaskExecutionFailed
from globus_compute_sdk.serialize import DillCodeSource


@lru_cache(maxsize=None)
def annotated_function():
    return "data"


def test_task_execution_failed_serialization_help_msg():
    try:
        DillCodeSource().serialize(annotated_function)
    except OSError:
        tb = traceback.format_exc()
    else:
        pytest.fail("Expected DillCodeSource to fail to serialize annotated functions")

    tef = TaskExecutionFailed(tb)
    assert "This appears to be an error with serialization." in str(tef)
    assert (
        "https://globus-compute.readthedocs.io/en/latest/sdk.html"
        "#specifying-a-serialization-strategy" in str(tef)
    )
