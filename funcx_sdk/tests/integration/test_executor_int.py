import os
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
