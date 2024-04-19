from __future__ import annotations

import uuid

class UUID(uuid.UUID):
    def __init__(self, inp: bytes | str) -> None:
        ...
