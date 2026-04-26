from __future__ import annotations


class IdFactory:
    PREFIX_FORMATS = {
        "SACCO": ("INST", 4),
        "MEMBER": ("MEM", 7),
        "ACCT": ("ACC", 8),
        "EXT": ("ACC", 8),
        "TXN": ("TXN", 12),
        "NODE": ("NODE", 8),
        "EDGE": ("EDGE", 8),
        "LOAN": ("LOAN", 6),
        "GUA": ("GUA", 6),
        "ALT": ("ALT", 8),
        "PAT": ("PAT", 8),
    }

    def __init__(self) -> None:
        self._counters: dict[str, int] = {}

    def next(self, prefix: str) -> str:
        value = self._counters.get(prefix, 0) + 1
        self._counters[prefix] = value
        label, width = self.PREFIX_FORMATS.get(prefix, (prefix, 6))
        return f"{label}{value:0{width}d}"

    @staticmethod
    def hash_id(prefix: str, value: object) -> str:
        import hashlib

        digest = hashlib.sha256(str(value).encode("utf-8")).hexdigest()[:16]
        return f"{prefix}_{digest}"
