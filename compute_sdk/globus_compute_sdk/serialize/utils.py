def pack_buffers(buffers: list[str]) -> str:
    """
    Combines a list of Compute-serialized buffers into a single string format.

    Each buffer is prefixed with its length in bytes followed by a newline,
    allowing unambiguous reconstruction of the original buffers.

    Parameters
    ----------
    buffers
        A list of serialized buffers
    """
    return "".join([f"{len(b)}\n{b}" for b in buffers])


def unpack_buffers(packed_buffer: str) -> list[str]:
    """
    Splits a packed buffer string into its constituent buffers.

    Parameters
    ----------
    packed_buffer
        A packed buffer string as produced by :func:`pack_buffers`.
    """
    unpacked = []
    while packed_buffer:
        try:
            delimiter, buffer = packed_buffer.split("\n", 1)
        except ValueError as e:
            raise ValueError("Missing newline delimiter") from e

        try:
            buf_len = int(delimiter)
        except ValueError as e:
            raise ValueError(f"Invalid length delimiter: {delimiter}") from e

        if buf_len < 0:
            raise ValueError(f"Length delimiter must be non-negative: {buf_len}")

        if len(buffer) < buf_len:
            raise ValueError(
                f"Truncated buffer: expected {buf_len} characters, but only"
                f" {len(buffer)} characters remain"
            )

        current, packed_buffer = buffer[:buf_len], buffer[buf_len:]
        unpacked.append(current)
    return unpacked
