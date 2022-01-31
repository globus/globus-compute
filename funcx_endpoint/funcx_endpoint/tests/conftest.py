import os
import uuid

import pytest
from parsl.providers import LocalProvider

from funcx_endpoint.executors import HighThroughputExecutor


@pytest.fixture(scope="module")
def htex():
    try:
        os.remove("interchange.log")
    except Exception:
        pass

    htex = HighThroughputExecutor(
        worker_debug=True,
        max_workers_per_node=2,
        passthrough=False,
        endpoint_id=str(uuid.uuid4()),
        available_accelerators=2,
        provider=LocalProvider(
            init_blocks=1,
            min_blocks=1,
            max_blocks=1,
        ),
        run_dir=".",
    )

    htex.start()
    yield htex
    htex.shutdown()
