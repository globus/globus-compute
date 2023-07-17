from __future__ import annotations

import multiprocessing
import random
import signal
import string
import time
import uuid
from queue import Queue

import globus_compute_sdk as gc
import globus_sdk
import pytest
import responses
from globus_compute_endpoint import engines
from globus_compute_sdk.sdk.web_client import WebClient


@pytest.fixture(autouse=True)
def verify_all_tests_reset_signals():
    orig_sig_handlers = [
        (sig, signal.getsignal(sig))
        for sig in range(1, 21)
        if sig not in (signal.SIGKILL, signal.SIGSTOP)  # the uncatchables
    ]
    yield
    handlers = [(sig, sigh, signal.getsignal(sig)) for sig, sigh in orig_sig_handlers]
    for sig, bef, aft in handlers:
        assert bef == aft, f"Signal {sig} not reset"


@pytest.fixture
def reset_signals():
    # As identified by an interaction with Parsl during our UTs, ensure that we
    # return the signals after each test.  Interprocess play is fun ...
    orig_sig_handlers = [
        (sig, signal.getsignal(sig))
        for sig in range(1, 21)
        if sig not in (signal.SIGKILL, signal.SIGSTOP)  # the uncatchables
    ]

    yield
    for sig, sigh in orig_sig_handlers:
        signal.signal(sig, sigh)


@pytest.fixture(scope="session")
def endpoint_uuid():
    return str(uuid.UUID(int=0))


@pytest.fixture(scope="session")
def default_endpoint_id():
    return str(uuid.UUID(int=1))


@pytest.fixture(scope="session")
def other_endpoint_id():
    return str(uuid.UUID(int=2))


@pytest.fixture(scope="session")
def tod_session_num():
    yield round(time.time()) % 86400


class FakeLoginManager:
    def ensure_logged_in(self) -> None:
        ...

    def logout(self) -> bool:
        ...

    def get_auth_client(self) -> globus_sdk.AuthClient:
        return globus_sdk.AuthClient(authorizer=globus_sdk.NullAuthorizer())

    def get_web_client(self, *, base_url: str | None = None) -> WebClient:
        return WebClient(
            base_url="https://compute.api.globus.org/v2/",
            authorizer=globus_sdk.NullAuthorizer(),
        )


@pytest.fixture
def get_standard_compute_client():
    responses.add(
        method=responses.GET,
        url="https://compute.api.globus.org/v2/version",
        headers={"Content-Type": "application/json"},
        json={"api": "0.4.0", "min_ep_version": "0.0.0", "min_sdk_version": "0.0.0"},
    )

    def func():
        return gc.Client(
            login_manager=FakeLoginManager(),
            do_version_check=False,
        )

    return func


###
# Engines
###


@pytest.fixture
def engine_heartbeat() -> float:
    return 0.1


@pytest.fixture
def proc_pool_engine(tmp_path, engine_heartbeat: float):
    ep_id = uuid.uuid4()
    queue = Queue()

    engine = engines.ProcessPoolEngine(
        heartbeat_period_s=engine_heartbeat, max_workers=2
    )
    engine.start(endpoint_id=ep_id, run_dir=str(tmp_path), results_passthrough=queue)

    yield engine
    engine.shutdown()


@pytest.fixture
def thread_pool_engine(tmp_path, engine_heartbeat: float):
    ep_id = uuid.uuid4()
    queue = Queue()

    engine = engines.ThreadPoolEngine(
        heartbeat_period_s=engine_heartbeat, max_workers=2
    )
    engine.start(endpoint_id=ep_id, run_dir=str(tmp_path), results_passthrough=queue)

    yield engine
    engine.shutdown()


@pytest.fixture
def gc_engine(tmp_path, engine_heartbeat: float):
    ep_id = uuid.uuid4()
    queue = multiprocessing.Queue()

    engine = engines.GlobusComputeEngine(
        address="127.0.0.1", heartbeat_period_s=engine_heartbeat, heartbeat_threshold=1
    )
    engine.start(endpoint_id=ep_id, run_dir=str(tmp_path), results_passthrough=queue)

    yield engine
    engine.shutdown()


###
# Misc
###


@pytest.fixture
def randomstring():
    def func(length=5, alphabet=string.ascii_letters):
        return "".join(random.choice(alphabet) for _ in range(length))

    return func


@pytest.fixture
def noop():
    def _wrapped(*args, **kwargs):
        pass

    return _wrapped
