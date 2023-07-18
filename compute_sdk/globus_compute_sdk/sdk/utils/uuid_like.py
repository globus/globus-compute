from __future__ import annotations

import typing as t
import uuid

# older pythons don't like aliases using |, even with a __future__ import
UUID_LIKE_T = t.Union[uuid.UUID, str]


def as_uuid(uuid_like: UUID_LIKE_T) -> uuid.UUID:
    return uuid_like if isinstance(uuid_like, uuid.UUID) else uuid.UUID(uuid_like)


def as_optional_uuid(optional_uuid_like: UUID_LIKE_T | None) -> uuid.UUID | None:
    return as_uuid(optional_uuid_like) if optional_uuid_like else None
