import os
import subprocess
import sys
import textwrap
from unittest import mock

import pytest
from tests.utils import try_assert

from funcx import FuncXClient
from funcx.sdk.executor import FuncXExecutor, _ResultWatcher


@pytest.mark.skipif(
    not os.getenv("FUNCX_INTEGRATION_TEST_WEB_URL"), reason="no integration web url"
)
def test_resultwatcher_graceful_shutdown():
    service_url = os.environ["FUNCX_INTEGRATION_TEST_WEB_URL"]
    fxc = FuncXClient(funcx_service_address=service_url)
    fxe = FuncXExecutor(funcx_client=fxc)
    rw = _ResultWatcher(fxe)
    rw._start_consuming = mock.Mock()
    rw.start()

    try_assert(lambda: rw._start_consuming.called)
    rw.shutdown()

    try_assert(lambda: rw._channel is None)
    try_assert(lambda: not rw._connection or rw._connection.is_closed)
    try_assert(lambda: not rw.is_alive())
    fxe.shutdown()


def test_executor_atexit_handler_catches_all_instances(tmp_path):
    test_script = tmp_path / "test.py"
    script_content = textwrap.dedent(
        """
        import random
        from funcx import FuncXExecutor
        from funcx.sdk.executor import _REGISTERED_FXEXECUTORS

        fxc = " a fake funcx_client"
        num_executors = random.randrange(1, 10)
        for i in range(num_executors):
            FuncXExecutor(funcx_client=fxc)  # start N threads, none shutdown
        fxe = FuncXExecutor(funcx_client=fxc)  # intentionally overwritten
        fxe = FuncXExecutor(funcx_client=fxc)

        num_executors += 2
        assert len(_REGISTERED_FXEXECUTORS) == num_executors, (
            f"Verify test setup: {len(_REGISTERED_FXEXECUTORS)} != {num_executors}"
        )
        fxe.shutdown()  # only shutting down _last_ instance.  Should still exit cleanly
        """
    )
    test_script.write_text(script_content)
    res = subprocess.run([sys.executable, str(test_script)], timeout=5)
    assert res.returncode == 0
