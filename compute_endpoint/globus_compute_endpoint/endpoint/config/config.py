from __future__ import annotations

import inspect
import logging
import os
import pathlib
import typing as t
import warnings

from globus_compute_endpoint.engines import GlobusComputeEngine
from globus_compute_endpoint.engines.base import GlobusComputeEngineBase
from globus_compute_sdk.sdk.utils.uuid_like import (
    UUID_LIKE_T,
    as_optional_uuid,
    as_uuid,
)

from ..utils import is_privileged
from .pam import PamConfiguration

MINIMUM_HEARTBEAT: float = 5.0
log = logging.getLogger(__name__)


class BaseConfig:
    """
    :param multi_user: If true, the endpoint will spawn child endpoint processes based
        upon a configuration template.

    :param display_name: The display name for the endpoint.  If ``None``, defaults to
        the endpoint name (i.e., the directory name in ``~/.globus_compute/``)

    :param allowed_functions: List of identifiers of functions that are allowed to be
        run on the endpoint

    :param authentication_policy: Endpoint users are evaluated against this Globus
        authentication policy

    :param subscription_id: Subscription ID associated with this endpoint

    :param amqp_port: Port to use for AMQP connections. Note that only 5671, 5672, and
        443 are supported by the Compute web services. If None, the port is assigned by
        the services (which default to 443).

    :param heartbeat_period: The interval (in seconds) at which heartbeat messages are
        sent from the endpoint to the Globus Compute web service

    :param environment: Environment the endpoint should connect to.  If not specified,
        the endpoint connects to production.  (Listed here for completeness, but only
        used internally by the dev team.)

    :param local_compute_services: Point the endpoint to a local instance of the Compute
        services.  (Listed here for completeness, but only used internally by the dev
        team.)

    :param debug: If set, emit debug-level log messages.  This is a configuration
        implementation of the CLI's ``--debug`` flag.  Note that if this value is
        explicitly False, then the CLI flag, if utilized, will still put the EP into
        "debug mode."  The CLI wins.
    """

    def __init__(
        self,
        *,
        multi_user: bool = False,
        high_assurance: bool = False,
        display_name: str | None = None,
        allowed_functions: t.Iterable[UUID_LIKE_T] | None = None,
        authentication_policy: UUID_LIKE_T | None = None,
        subscription_id: UUID_LIKE_T | None = None,
        amqp_port: int | None = None,
        heartbeat_period: float | int = 30,
        environment: str | None = None,
        local_compute_services: bool = False,
        debug: bool = False,
    ):
        # Misc
        self.display_name = display_name
        self.debug = debug is True
        self.multi_user = multi_user is True
        self.high_assurance = high_assurance is True

        # Connection info and tuning
        self.amqp_port = amqp_port
        self.heartbeat_period = heartbeat_period
        self.environment = environment
        self.local_compute_services = local_compute_services is True

        # Auth
        self.allowed_functions = allowed_functions
        self.authentication_policy = authentication_policy
        self.subscription_id = subscription_id

        # Used to store the raw content of the YAML or Python config file
        self.source_content: str | None = None

    def __repr__(self) -> str:
        kwds: dict[str, t.Any] = {}
        for cls in type(self).__mro__:
            fargspec = inspect.getfullargspec(cls.__init__)  # type: ignore[misc]
            kwdefs = fargspec.kwonlydefaults
            for kw in fargspec.kwonlyargs:
                if kw == "executors":
                    # special case; remove when deprecation complete and removed
                    # circa Aug, 2025 (but no rush)
                    continue
                curval = getattr(self, kw)
                if kwdefs and curval != kwdefs.get(kw):
                    kwds.setdefault(kw, curval)

        self_name = type(self).__name__
        kwargs = (f"{k}={repr(v)}" for k, v in sorted(kwds.items()))
        return f"{self_name}({', '.join(kwargs)})"

    @property
    def heartbeat_period(self):
        return self._heartbeat_period

    @heartbeat_period.setter
    def heartbeat_period(self, val: float | int):
        if val < MINIMUM_HEARTBEAT:
            log.warning(f"Heartbeat minimum is {MINIMUM_HEARTBEAT}s (requested: {val})")
        self._heartbeat_period = max(MINIMUM_HEARTBEAT, val)

    @property
    def allowed_functions(self):
        if self._allowed_functions is not None:
            return tuple(map(str, self._allowed_functions))
        return None

    @allowed_functions.setter
    def allowed_functions(self, val: t.Iterable[UUID_LIKE_T] | None):
        if val is None:
            self._allowed_functions = None
        else:
            self._allowed_functions = tuple(as_uuid(f_uuid) for f_uuid in val)

    @property
    def authentication_policy(self):
        return self._authentication_policy and str(self._authentication_policy) or None

    @authentication_policy.setter
    def authentication_policy(self, val: UUID_LIKE_T | None):
        self._authentication_policy = as_optional_uuid(val)

    @property
    def subscription_id(self):
        return self._subscription_id and str(self._subscription_id) or None

    @subscription_id.setter
    def subscription_id(self, val: UUID_LIKE_T | None):
        self._subscription_id = as_optional_uuid(val)


class UserEndpointConfig(BaseConfig):
    """Holds the configuration items for a task-processing endpoint.

    Typically, one does not instantiate this configuration directly, but specifies
    the relevant options in the endpoint's ``config.yaml`` file.  For example, to
    specify an endpoint that only uses threads on the endpoint host, the configuration
    might look like:

    .. code-block:: yaml
       :caption: ``config.yaml``

       display_name: My single-block, host-only EP at site ABC
       engine:
         type: GlobusComputeEngine
         provider:
           type: LocalProvider
           max_blocks: 1

    Please see the |BaseConfig| class for a list of options that both
    |ManagerEndpointConfig| and |UserEndpointConfig| classes share.

    :param engine: The GlobusComputeEngine for this endpoint to execute functions.
        The currently known engines are ``GlobusComputeEngine``, ``ProcessPoolEngine``,
        and ``ThreadPoolEngine``.  See :ref:`uep_conf` for more information.

    :param executors: A tuple of executors which serve as the backend for function
        execution.  If ``None``, then use a default-instantiation of
        ``GlobusComputeEngine``.

        N.B. this field, for historical reasons, requires an iterable.  However,
        Compute only supports a single engine.

        Deprecated; use ``engine`` instead

    :param heartbeat_threshold: Seconds since the last heartbeat message from the
        Globus Compute web service after which the connection is assumed to be
        disconnected.

    :param idle_heartbeats_soft: Number of heartbeats after an endpoint is idle (no
        outstanding tasks or results, and at least 1 task or result has been
        forwarded) before the endpoint shuts down.  If 0, then the endpoint must be
        manually triggered to shut down (e.g., SIGINT [Ctrl+C] or SIGTERM).

    :param idle_heartbeats_hard: Number of heartbeats after no task or result has moved
        before the endpoint shuts down.  Unlike ``idle_heartbeats_soft``, this idle
        timer does not require that there are no outstanding tasks or results.  If no
        task or result has moved in this many heartbeats, then the endpoint will shut
        down.  In particular, this is intended to catch the error condition that a
        worker has gone missing and will thus *never* return the task it was sent.
        Note that this setting is only enabled if the ``idle_heartbeats_soft`` is a
        value greater than 0.  Suggested value: a multiplier of heartbeat_period
        equivalent to two days.  For example, if ``heartbeat_period`` is 30s, then
        suggest 5760.

    :param detach_endpoint: Whether the endpoint daemon be run as a detached process.
        This is good for a real edge node, but an anti-pattern for kubernetes pods

    :param endpoint_setup: Command(s) to be run during the endpoint initialization
        process

    :param endpoint_teardown: Command(s) to be run during the endpoint
        shutdown process

    :param log_dir: path to the top-level directory where logs should be written

    :param stdout: Path where the endpoint's stdout should be written

    :param stderr: Path where the endpoint's stderr should be written
    """

    def __init__(
        self,
        *,
        # Execution backend
        engine: GlobusComputeEngineBase | None = None,
        executors: t.Iterable[GlobusComputeEngineBase] | None = None,
        # Tuning info
        heartbeat_threshold: int = 120,
        idle_heartbeats_soft: int = 0,
        idle_heartbeats_hard: int = 5760,  # Two days, divided by `heartbeat_period`
        detach_endpoint: bool = True,
        endpoint_setup: str | None = None,
        endpoint_teardown: str | None = None,
        # Logging info
        log_dir: str | None = None,
        stdout: str = "./endpoint.log",
        stderr: str = "./endpoint.log",
        **kwargs,
    ) -> None:
        kwargs["multi_user"] = False
        super().__init__(**kwargs)

        if executors and engine:
            raise ValueError("Only one of `engine` or `executors` may be specified")
        elif executors:
            _e = tuple(e for e in executors)[:1]
            assert len(_e) == 1, "Oh, mypy.  We *just proved* that it's 1.  :facepalm:"
            self.executors = _e
        else:
            self.engine = engine

        # Single-user tuning
        self.heartbeat_threshold = heartbeat_threshold
        self.idle_heartbeats_soft = int(max(0, idle_heartbeats_soft))
        self.idle_heartbeats_hard = int(max(0, idle_heartbeats_hard))
        self.detach_endpoint = detach_endpoint

        self.endpoint_setup = endpoint_setup
        self.endpoint_teardown = endpoint_teardown

        # Logging info
        self.log_dir = log_dir
        self.stdout = stdout
        self.stderr = stderr

    @property
    def engine(self) -> GlobusComputeEngineBase | None:
        return self._engine

    @engine.setter
    def engine(self, val: GlobusComputeEngineBase | None):
        self._engine = val

    @property
    def executors(self) -> tuple[GlobusComputeEngineBase | None]:
        warnings.warn("`executors` is deprecated; use `engine`", stacklevel=2)
        return (self.engine,)

    @executors.setter
    def executors(self, val: t.Iterable[GlobusComputeEngineBase] | None):
        warnings.warn("`executors` is deprecated; use `engine`", stacklevel=2)
        if val is None:
            self.engine = GlobusComputeEngine()
        else:
            self.engine = tuple(e for e in val)[0]

    @property
    def heartbeat_threshold(self):
        return self._heartbeat_threshold

    @heartbeat_threshold.setter
    def heartbeat_threshold(self, val: int):
        self._heartbeat_threshold = max(self.heartbeat_period * 2, val)

    @property
    def idle_heartbeats_soft(self) -> int:
        return self._idle_heartbeats_soft

    @idle_heartbeats_soft.setter
    def idle_heartbeats_soft(self, val: int):
        self._idle_heartbeats_soft = max(0, val)

    @property
    def idle_heartbeats_hard(self) -> int:
        return self._idle_heartbeats_hard

    @idle_heartbeats_hard.setter
    def idle_heartbeats_hard(self, val: int):
        self._idle_heartbeats_hard = max(0, val)


# for backwards compatibility
Config = UserEndpointConfig


class ManagerEndpointConfig(BaseConfig):
    """Holds the configuration items for an endpoint manager.

    Typically, one does not instantiate this configuration directly, but specifies
    the relevant options in the endpoint's ``config.yaml`` file.  For example, to
    specify an endpoint as multi-user (this class) the YAML config might be just 1
    line:

    .. code-block:: yaml
       :caption: ``config.yaml``

       multi_user: true

    Note that for multi-user endpoints that will not be run with privileges, identity
    mapping is disabled (hence not specified above).  Conversely, if the process will
    have elevated privileges (e.g., run by ``root`` user or has |setuid(2)|_
    privileges) then identity mapping is required:

    .. code-block:: yaml
       :caption: ``config.yaml`` (for a ``root``-owned process)

       display_name: Debug queue, 1-block max
       multi_user: true
       identity_mapping_config_path: /path/to/this/idmap_conf.json

    Please see the |BaseConfig| class for a list of options that both
    |ManagerEndpointConfig| and |UserEndpointConfig| classes share.

    :param public: Whether all users can discover the multi-user endpoint via the
        `Globus Compute web user interface <https://app.globus.org/compute>`_ and API.

        .. warning::

           Do not use this flag as a means of security.  It controls *visibility* in
           the web user interface.  It does **not control access** to the endpoint.

    :param user_config_template_path: Path to the user configuration template file for
        this endpoint. If not specified, the default template path will be used.

    :param user_config_schema_path: Path to the user configuration schema file for this
        endpoint. If not specified, the default schema path will be used.

    :param identity_mapping_config_path: Path to the identity mapping configuration for
        this endpoint.  If the process is not privileged, a warning will be emitted to
        the logs and this item will be ignored; conversely, if privileged, this
        configuration item is required, and a ``ValueError`` will be raised if the path
        does not exist.

    :param audit_log_path: Path to the audit log.  If specified, and the endpoint is
        marked as High-Assurance (HA), then the MEP will write auditing records here.
        An auditing record is a single-line of text, received from child endpoints at
        "interesting" points in a task lifetime.  For example, when the UEP interchange
        first receives a task, it will emit an auditing record of ``RECEIVED`` for that
        task.  Similarly, the UEP will emit ``EXEC_START`` when the executor registers
        the task, ``RUNNING`` if the executor shares that the task is running, and
        ``EXEC_END`` when the task is complete.  If the path is created, it will be
        created with user-secure permission (``umask=0o077``), but it not be checked
        for permission conformance thereafter.  It is up to the administrator to ensure
        this path is generally secured.

    :param pam: Whether to enable authorization of user-endpoints via PAM routines, and
        optionally specify the PAM service name.  See |PamConfiguration|.  If not
        specified, PAM authorization defaults to disabled.

    :param mu_child_ep_grace_period_s: The web-services send a start-user-endpoint to
        the endpoint manager ahead of tasks for the target user endpoint.  If the
        user-endpoint is already running, these requests are ignored.  To account for
        the inherent race-condition of receiving a start request just before the
        user-endpoint shuts down, the endpoint manager will hold on to the most recent
        start request for the user-endpoint for this grace period.

    :param force_mu_allow_same_user:  If set, override the heuristic that determines
        whether the UID running the multi-user endpoint may also run single-user
        endpoints.

        Normally, the multi-user endpoint disallows starting single-user endpoints with
        the same UID as the parent process unless the UID has no privileges.  In other
        words, ``root`` may not process tasks.  This flag is for those niche setups that
        require the ``root`` user to process tasks.  Be very careful if setting this
        flag.

    .. |BaseConfig| replace:: :class:`BaseConfig <globus_compute_endpoint.endpoint.config.config.BaseConfig>`
    .. |ManagerEndpointConfig| replace:: :class:`ManagerEndpointConfig <globus_compute_endpoint.endpoint.config.config.ManagerEndpointConfig>`
    .. |UserEndpointConfig| replace:: :class:`UserEndpointConfig <globus_compute_endpoint.endpoint.config.config.UserEndpointConfig>`
    .. |PamConfiguration| replace:: :class:`PamConfiguration <globus_compute_endpoint.endpoint.config.pam.PamConfiguration>`

    .. |setuid(2)| replace:: ``setuid(2)``
    .. _setuid(2): https://www.man7.org/linux/man-pages/man2/setuid.2.html
    """  # noqa

    def __init__(
        self,
        *,
        public: bool = False,
        user_config_template_path: os.PathLike | str | None = None,
        user_config_schema_path: os.PathLike | str | None = None,
        identity_mapping_config_path: os.PathLike | str | None = None,
        audit_log_path: os.PathLike | str | None = None,
        pam: PamConfiguration | None = None,
        force_mu_allow_same_user: bool = False,
        mu_child_ep_grace_period_s: float = 30.0,
        **kwargs,
    ):
        kwargs["multi_user"] = True
        super().__init__(**kwargs)
        self.public = public is True

        # Identity mapping
        self.force_mu_allow_same_user = force_mu_allow_same_user is True
        self.mu_child_ep_grace_period_s = mu_child_ep_grace_period_s

        _tmp = user_config_template_path  # work with both mypy and flake8
        self.user_config_template_path = _tmp  # type: ignore[assignment]

        _tmp = user_config_schema_path  # work with both mypy and flake8
        self.user_config_schema_path = _tmp  # type: ignore[assignment]

        _tmp = identity_mapping_config_path  # work with both mypy and flake8
        self.identity_mapping_config_path = _tmp  # type: ignore[assignment]

        _tmp = audit_log_path  # work with both mypy and flake8
        self.audit_log_path = _tmp  # type: ignore[assignment]

        self.pam = pam or PamConfiguration(enable=False)

    @property
    def user_config_template_path(self) -> pathlib.Path | None:
        return self._user_config_template_path

    @user_config_template_path.setter
    def user_config_template_path(self, val: os.PathLike | str | None):
        self._user_config_template_path = pathlib.Path(val) if val else None

    @property
    def user_config_schema_path(self) -> pathlib.Path | None:
        return self._user_config_schema_path

    @user_config_schema_path.setter
    def user_config_schema_path(self, val: os.PathLike | str | None):
        self._user_config_schema_path = pathlib.Path(val) if val else None

    @property
    def identity_mapping_config_path(self) -> pathlib.Path | None:
        return self._identity_mapping_config_path

    @identity_mapping_config_path.setter
    def identity_mapping_config_path(self, val: os.PathLike | str | None):
        self._identity_mapping_config_path: pathlib.Path | None
        if is_privileged():
            if not val:
                raise ValueError(
                    "Identity mapping required.  (Hint: identity_mapping_config_path)"
                )

            _p = pathlib.Path(val)
            if not _p.exists():
                raise ValueError(f"Identity mapping config path not found ({_p})")
            self._identity_mapping_config_path = _p
        else:
            self._identity_mapping_config_path = None
            if val:
                log.warning(
                    "Identity mapping specified, but process is not privileged;"
                    " ignoring identity mapping configuration."
                )

    @property
    def audit_log_path(self) -> pathlib.Path | None:
        return self._audit_log_path

    @audit_log_path.setter
    def audit_log_path(self, val: os.PathLike | str | None):
        self._audit_log_path = pathlib.Path(val) if val else None
