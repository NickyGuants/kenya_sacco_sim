from __future__ import annotations


class IdFactory:
    def __init__(self) -> None:
        self._counters: dict[str, int] = {}

    def next(self, prefix: str) -> str:
        value = self._counters.get(prefix, 0) + 1
        self._counters[prefix] = value
        return f"{prefix}_{value:06d}"

    @staticmethod
    def hash_id(prefix: str, value: object) -> str:
        import hashlib

        digest = hashlib.sha256(str(value).encode("utf-8")).hexdigest()[:16]
        return f"{prefix}_{digest}"
