import typing as t
from itertools import islice


def chunk_by(iterable: t.Iterable, size) -> t.Iterable[tuple]:
    to_chunk_iter = iter(iterable)
    return iter(lambda: tuple(islice(to_chunk_iter, size)), ())
