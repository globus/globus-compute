from __future__ import annotations

import json
import logging
import os
import pathlib
import threading
import typing as t

from globus_identity_mapping.loader import load_mappers
from globus_identity_mapping.protocol import IdentityMappingProtocol

log = logging.getLogger(__name__)

# From: https://docs.globus.org/globus-connect-server/v5.4/identity-mapping-guide/#command_line_options  # noqa
POSIX_CONNECTOR_ID = "145812c8-decc-41f1-83cf-bb2a85a2a70b"


class PosixIdentityMapper:
    """
    IdentityMapper is a wrapper around IdentityMappingProtocol implementations,
    performing two main functions:

        - auto-loading the correct IdentityMappingProtocol implementation class,
          based on a configuration file's defined DATA_TYPE.  (See
          https://docs.globus.org/globus-connect-server/v5.4/identity-mapping-guide/ )
        - atomically updating the loaded configuration without requiring an
          application restart

    To construct, specify an identity-mapping configuration in a JSON file and
    then pass the path to the constructor with a tool or utility identifier.
    Example:

        >>> import json, pathlib
        >>> from globus_compute_endpoint.endpoint.identity_mapper import PosixIdentityMapper  # noqa

        >>> identity_mapping_conf_path = "some/identity_mapping_configuration.json"
        >>> im_path = pathlib.Path(identity_mapping_conf_path)  # for convenience
        >>> print(im_path.read_text())
        [
          {
            "DATA_TYPE": "expression_identity_mapping#1.0.0",
            "mappings": [
              {
                "source": "{username}",
                "match": "(.*)@university\\.example\\.edu",
                "output": "{0}"
              }
            ]
          }
        ]

        >>> some_utility_identifier = "some identifier"
        >>> im = PosixIdentityMapper(
        ...    identity_mapping_conf_path, some_utility_identifier
        ... )

    Currently, the DATA_TYPE must map to an implementing class in the
    ``globus-identity-mapping`` Python library.

    Now any valid Globus identity can be mapped to a local user via either the
    ``.map_identity()`` method:

        >>> globus_identity_set = json.loads(  # usually collected from an API ...
        ... '''
        ... [
        ...     {
        ...         "id": "1c6dbe57-4a3e-44e5-826e-0280003585ae",
        ...         "sub": "1c6dbe57-4a3e-44e5-826e-0280003585ae",
        ...         "organization": "Example Widgets, Co",
        ...         "name": "Jessie Jess",
        ...         "username": "jess@example.org",
        ...         "identity_provider": "62dc25b7-c693-4528-831b-4557a7a0d1e4",
        ...         "identity_provider_display_name": "Example Identities, Ltd",
        ...         "email": "jess@example.org",
        ...         "last_authentication": 1029384756
        ...     },
        ...     {
        ...         "id": "d427502d-722b-4361-ad34-9327010a7b81",
        ...         "sub": "d427502d-722b-4361-ad34-9327010a7b81",
        ...         "name": "Jessie Jess",
        ...         "username": "jessica.jazz.jess@university.example.edu",
        ...         "identity_provider": "09a7033a-b53b-44e2-8c71-b60f0468df07",
        ...         "identity_provider_display_name": "University of Example",
        ...         "email": "jessica.jess@example-2.com",
        ...         "last_authentication": 1629384750
        ...     }
        ... ]
        ... '''
        ... )

        >>> im.map_identity(globus_identity_set)
        'jessica.jazz.jess'

    Meanwhile, if requirements change and the identity mapping configuration
    must be changed, the changes will be atomically discovered:

        >>> im_path.write_text('''
        ... [
        ...   {
        ...     "DATA_TYPE": "expression_identity_mapping#1.0.0",
        ...     "mappings": [
        ...       {
        ...         "source": "{username}",
        ...         "match": "(.*)@example\\.org",
        ...         "output": "{0}"
        ...       }
        ...     ]
        ...   }
        ... ]
        ... ''')
        >>> time.sleep(im.poll_interval_s)  # for example, wait until reloaded
        >>> im.map_identity(globus_identity_set)
        'jess'


    """

    def __init__(
        self,
        identity_mapping_config_path: os.PathLike,
        endpoint_identifier: str,
        poll_interval_s: float = 5.0,
    ):
        """
        :param identity_mapping_config_path:
        :param endpoint_identifier:
        :param poll_interval_s:
        """
        self._time_to_stop = threading.Event()
        self.config_path = pathlib.Path(identity_mapping_config_path)
        self._config_stat = self.config_path.stat()
        self._endpoint_identifier = endpoint_identifier
        self.poll_interval_s = poll_interval_s

        self._identity_mappings: list[IdentityMappingProtocol] = []

        self.load_configuration()  # Executed on main thread

        threading.Thread(target=self._poll_config, daemon=True).start()

    def __del__(self):
        self.stop_watching()

    def stop_watching(self):
        self._time_to_stop.set()

    @property
    def poll_interval_s(self) -> float:
        return self._poll_interval_s

    @poll_interval_s.setter
    def poll_interval_s(self, new_interval_s: float) -> None:
        self._poll_interval_s = max(0.5, new_interval_s)

    def _poll_config(self):
        fail_msg_fmt = (
            "Unable to update identity configuration -- ({}) {}"
            f"\n  Identity configuration path: {self.config_path}"
        )

        while not self._time_to_stop.wait(self.poll_interval_s):
            try:
                self._update_if_config_changed()
            except Exception as e:
                msg = fail_msg_fmt.format(type(e).__name__, e)
                log.debug(msg, exc_info=e)
                log.error(msg)
        log.debug("Polling thread stops")

    def _update_if_config_changed(self):
        cstat = self._config_stat  # "current stat"
        nstat = self.config_path.stat()  # "new stat"
        cur_s = (cstat.st_ino, cstat.st_ctime, cstat.st_mtime, cstat.st_size)
        new_s = (nstat.st_ino, nstat.st_ctime, nstat.st_mtime, nstat.st_size)

        if cur_s == new_s:
            # no change; suggests the file has *also* not changed
            return

        log.info(
            "Identity mapping configuration change detected; rereading:"
            f" {self.config_path}"
        )
        self.load_configuration()

    def load_configuration(self):
        self._config_stat = self.config_path.stat()
        self.identity_mappings = json.loads(self.config_path.read_bytes())

    @property
    def identity_mappings(self) -> list[IdentityMappingProtocol]:
        return self._identity_mappings

    @identity_mappings.setter
    def identity_mappings(self, mappings_list: t.Iterable[dict] | None):
        self._identity_mappings = load_mappers(
            mappings_list, POSIX_CONNECTOR_ID, self._endpoint_identifier
        )

    @identity_mappings.deleter
    def identity_mappings(self):
        self.identity_mappings = None

    def map_identities(
        self, identity_set: t.Collection[t.Mapping[str, str]]
    ) -> list[list[dict[str, list[str]]]]:
        """
        Return a list of successful mappings; the index in the top-level list is the
        index of the mapper in `self.identity_mappings`.  The sublevel lists contain
        dictionaries of each identity's successful mappings (possibly plural) by that
        mapper.

        Every mapper will have at least one entry in the top-level list, though if it
        failed to find a mapping, the list will be empty.  Example return value:

            [
                [],  # self.identity_mappings[0] found no mapping
                [
                    {"uuid_str1": ["alice", "bob"]},
                    {"uuid_str3": ["charlie"]}
                ],   # self.identity_mappings[1] found 3 mappings
                [
                    {"uuid_str3": ["charlie"]}
                ]    # self.identity_mappings[2] found 1 mapping
            ]
        """
        results: list[list[dict[str, list[str]]]] = []
        num_mappers = len(self.identity_mappings)
        for m_i, mapper in enumerate(self.identity_mappings, start=1):
            results.append([])
            try:
                for mapping in mapper.map_identities(identity_set):
                    if mapping:
                        results[-1].append(mapping)
            except Exception as e:
                log.warning(
                    f"Identity mapper {m_i} [of {num_mappers}]"
                    f" ({type(mapper).__name__}) failed -- ({type(e).__name__}) {e}"
                )

        return results
