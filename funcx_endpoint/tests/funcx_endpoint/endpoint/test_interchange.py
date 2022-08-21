import logging
from importlib.machinery import SourceFileLoader

import pika
import pytest

from funcx_endpoint.endpoint.endpoint import Endpoint
from funcx_endpoint.endpoint.interchange import EndpointInterchange

logger = logging.getLogger("mock_funcx")


@pytest.fixture
def funcx_dir(tmp_path):
    fxdir = tmp_path / "funcx"
    fxdir.mkdir()
    yield fxdir


class TestStart:
    def test_endpoint_id(self, mocker, funcx_dir):
        mock_client = mocker.patch("funcx_endpoint.endpoint.interchange.FuncXClient")
        mock_client.return_value = None

        manager = Endpoint(funcx_dir=str(funcx_dir))

        manager.configure_endpoint("mock_endpoint", None)
        endpoint_config = SourceFileLoader(
            "config", str(funcx_dir / "mock_endpoint" / "config.py")
        ).load_module()

        for executor in endpoint_config.config.executors:
            executor.passthrough = False

        ic = EndpointInterchange(
            endpoint_config.config,
            reg_info=None,
            endpoint_id="mock_endpoint_id",
        )

        for executor in ic.executors.values():
            assert executor.endpoint_id == "mock_endpoint_id"

    def test_start_no_reg_info(self, mocker, funcx_dir):
        def _fake_retry(func, *args, **kwargs):
            return func()

        mocker.patch("funcx_endpoint.endpoint.interchange.retry_call", _fake_retry)

        mock_client = mocker.patch("funcx_endpoint.endpoint.interchange.FuncXClient")
        mock_client.return_value = None

        mock_register_endpoint = mocker.patch(
            "funcx_endpoint.endpoint.interchange.register_endpoint"
        )
        result_url = "amqp://a.sdf"  # just a filler text for this test; don't ...
        task_url = "amqp://a.sdf"  # ... mistakenly potentially test the wrong thing
        mock_register_endpoint.return_value = (
            {
                "exchange_name": "results",
                "exchange_type": "topic",
                "result_url": result_url,
                "pika_conn_params": pika.URLParameters(result_url),
            },
            {
                "exchange_name": "tasks",
                "exchange_type": "direct",
                "task_url": task_url,
                "pika_conn_params": pika.URLParameters(task_url),
            },
        )

        manager = Endpoint(funcx_dir=funcx_dir)

        manager.configure_endpoint("mock_endpoint", None)
        endpoint_config = SourceFileLoader(
            "config", str(funcx_dir / "mock_endpoint" / "config.py")
        ).load_module()

        for executor in endpoint_config.config.executors:
            executor.passthrough = False

        mock_quiesce = mocker.patch.object(
            EndpointInterchange, "quiesce", return_value=None
        )
        mock_main_loop = mocker.patch.object(
            EndpointInterchange, "_main_loop", return_value=None
        )

        ic = EndpointInterchange(
            config=endpoint_config.config,
            reg_info=None,
            endpoint_id="mock_endpoint_id",
        )
        ic._kill_event = mocker.Mock()
        ic._kill_event.is_set.side_effect = (False, True)  # Loop only once

        ic.results_outgoing = mocker.Mock()

        ic.start()
        assert ic._task_puller_proc.is_alive()
        ic._quiesce_event.set()
        ic._task_puller_proc.join()

        # we need to ensure that retry_call is called during interchange
        # start if reg_info has not been passed into the interchange
        mock_quiesce.assert_called()
        mock_main_loop.assert_called()
        mock_register_endpoint.assert_called()
