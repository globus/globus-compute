from __future__ import annotations

import os
import pathlib
import sys
import typing as t


class ResultStore:
    """
    Persists bytes to disk by key via a dict()-like API.

    The ResultStore is a generic byte-storage data structure that stores and
    retrieves bytes to the filesystem.  Each payload is stored on the
    filesystem as a single file, named by the associated key.  Payloads are
    stored in a subdirectory of the init path, named "unacked_results".

    A typical interaction might look like:

        >>> rs = ResultStore(endpoint_dir="some_dir")
        >>> rs["some_key_01"] = b"some_payload_01"

    Access a single result by the result key

        >>> stored_bytes = rs["some_key_01"]  # raises if key does not exist
        >>> stored_bytes = rs.get("some_key_01")  # returns None if key doesn't exist

    Iterate all currently stored items via iteration:

        >>> for key_str, stored_bytes in rs:

    Discard stored items via `.pop()`, `.remove()`, or `.discard()`

        >>> rs.pop("some_key_01")  # also returns result, but ignored here
        >>> rs.pop("some_key_01")  # will raise as key does not exist
        >>> rs.discard("some_key_02")  # will not raise if key does not exist

    Completely empty the ResultStore with `.clear()`.  All non-hidden files
    (those not beginning with a dot) in the unacked_results directory will be
    removed from the filesystem.

        >>> rs.clear()
    """

    def __init__(self, endpoint_dir: str | pathlib.Path):
        self.endpoint_dir = pathlib.Path(endpoint_dir)
        self.data_path = self.endpoint_dir / "unacked_results"
        self.data_path.mkdir(exist_ok=True)

        self.data_path.chmod(mode=0o0700)
        test_path = self.data_path / ".test-reading-and-writing.txt"
        test_path.write_bytes(b"Verify write-ability")
        if not any(i for i in self.data_path.glob(".test*")):
            msg = (
                f"Unable to list files; does {self.data_path} have the correct"
                " permissions?"
            )
            raise PermissionError(msg)
        test_path.unlink()

    def __contains__(self, key: str) -> bool:
        normpath = os.path.normpath(key)
        return (self.data_path / normpath).exists()

    def __iter__(self) -> t.Iterable[tuple[str, bytes]]:
        for rp in self._iter_result_paths():
            yield rp.name, rp.read_bytes()

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(data_dir: {self.data_path})"

    def __getitem__(self, key):
        rp = self.data_path / key
        return rp.read_bytes()

    def __setitem__(self, key: str, payload: bytes) -> None:
        """
        Store bytes to disk by filename 'key'
        """
        result_path = self.data_path / os.path.normpath(key)
        result_path.write_bytes(payload)

    def __delitem__(self, key: str) -> None:
        """
        Remove the payload corresponding to key from the store.

        If there is no payload for key, raise a FileNotFoundError.
        """
        (self.data_path / key).unlink()

    def discard(self, key: str) -> None:
        """
        Discard the requested task result.  Will not raise if backing file does
        not exist.

        Parameters
        ----------
        key - the key for the result
        """
        if sys.version_info < (3, 8):
            try:
                (self.data_path / key).unlink()
            except FileNotFoundError:
                pass
        else:
            (self.data_path / key).unlink(missing_ok=True)

    def _iter_result_paths(self) -> t.Iterable[pathlib.Path]:
        yield from self.data_path.glob("[!.]*")

    def get(self, key: str, default=None) -> bytes:
        """
        Retrieve result from the data directory.

        Raises
        ------
        FileNotFoundError when backing file does not exist

        Returns
        -------
        message
        """
        try:
            rp = self.data_path / key
            return rp.read_bytes()
        except FileNotFoundError:
            return default

    def pop(self, key: str, *args, **kwargs) -> bytes:
        """
        Retrieve and remove a result from the data directory.

        Raises
        ------
        FileNotFoundError when backing file does not exist

        Returns
        -------
        message
        """
        try:
            payload = self[key]
            self.discard(key)
            return payload
        except FileNotFoundError:
            if args:
                return args[0]
            elif "default" in kwargs:
                return kwargs["default"]
            raise

    def clear(self) -> None:
        """
        Remove all stored items from the storage directory.  This iterates the
        storage for all non-hidden files (those that don't begin with a dot)
        and safely unlinks them.
        """
        if sys.version_info < (3, 8):
            for result_path in self._iter_result_paths():
                try:
                    result_path.unlink()
                except FileNotFoundError:
                    pass
        else:
            for result_path in self._iter_result_paths():
                result_path.unlink(missing_ok=True)
