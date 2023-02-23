import pathlib
import threading
import uuid
from importlib.machinery import SourceFileLoader

import pytest
from funcx_common.messagepack import pack, unpack
from funcx_common.messagepack.message_types import Result, Task
from tests.utils import try_for_timeout

from funcx_endpoint.endpoint.endpoint import Endpoint
from funcx_endpoint.endpoint.interchange import EndpointInterchange
from funcx_endpoint.endpoint.utils.config import Config

_MOCK_BASE = "funcx_endpoint.endpoint.interchange."


@pytest.fixture
def funcx_dir(tmp_path):
    fxdir = tmp_path / pathlib.Path("funcx")
    fxdir.mkdir()
    yield fxdir


def test_endpoint_id(funcx_dir):
    manager = Endpoint()
    config_dir = funcx_dir / "mock_endpoint"

    manager.configure_endpoint(config_dir, None)
    endpoint_config = SourceFileLoader(
        "config", str(funcx_dir / "mock_endpoint" / "config.py")
    ).load_module()

    for executor in endpoint_config.config.executors:
        executor.passthrough = False

    ic = EndpointInterchange(
        endpoint_config.config,
        reg_info={"task_queue_info": {}, "result_queue_info": {}},
        endpoint_id="mock_endpoint_id",
    )

    ic.start()
    assert ic.executor.endpoint_id == "mock_endpoint_id"


def test_start_requires_pre_registered(funcx_dir):
    with pytest.raises(TypeError):
        EndpointInterchange(
            config=Config(),
            reg_info=None,
            endpoint_id="mock_endpoint_id",
        )


def test_invalid_message_result_returned(mocker):
    ei = EndpointInterchange(
        config=Config(executors=[mocker.Mock(endpoint_id=None)]),
        reg_info={"task_queue_info": {}, "result_queue_info": {}},
    )

    mock_results = mocker.MagicMock()
    mocker.patch(f"{_MOCK_BASE}ResultQueuePublisher", return_value=mock_results)
    mocker.patch(f"{_MOCK_BASE}convert_to_internaltask", side_effect=Exception("BLAR"))
    task = Task(task_id=uuid.uuid4(), task_buffer="")
    ei.pending_task_queue.put(pack(task))
    t = threading.Thread(target=ei._main_loop, daemon=True)
    t.start()

    try_for_timeout(lambda: mock_results.publish.called, timeout_ms=1000)
    ei.time_to_quit = True
    t.join()

    assert mock_results.publish.called
    msg = mock_results.publish.call_args[0][0]
    result: Result = unpack(msg)
    assert result.task_id == task.task_id
    assert "Failed to start task" in result.data
