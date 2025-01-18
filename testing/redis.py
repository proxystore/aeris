from __future__ import annotations

from typing import Any


class MockRedis:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.values: dict[str, str] = {}
        self.lists: dict[str, list[str]] = {}

    def blpop(
        self,
        *keys: str,
        timeout: int = 0,
    ) -> tuple[str, ...] | None:
        result: list[str] = []
        for key in keys:
            if key not in self.lists or len(self.lists[key]) == 0:
                return None
            result.extend([key, self.lists[key].pop()])
        return tuple(result)

    def close(self) -> None:
        pass

    def delete(self, key: str) -> None:  # pragma: no cover
        if key in self.values:
            del self.values[key]
        elif key in self.lists:
            del self.lists[key]

    def exists(self, key: str) -> bool:  # pragma: no cover
        return key in self.values or key in self.lists

    def get(self, key: str) -> str | list[str] | None:  # pragma: no cover
        if key in self.values:
            return self.values[key]
        elif key in self.lists:
            return self.lists[key]
        return None

    def rpush(self, key: str, *values: str) -> None:
        if key not in self.lists:
            self.lists[key] = []
        self.lists[key].extend(values)

    def set(self, key: str, value: str) -> None:
        self.values[key] = value
