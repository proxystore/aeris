from __future__ import annotations

from concurrent.futures import Future
from typing import Any
from typing import TYPE_CHECKING
from typing import TypeVar

from aeris.identifier import AgentIdentifier

if TYPE_CHECKING:
    from aeris.exchange import Exchange

T = TypeVar('T')


class Handle:
    def __init__(self, aid: AgentIdentifier, exchange: Exchange) -> None:
        self.aid = aid
        self.exchange = exchange

    def invoke(
        self,
        action: str,
        /,
        *args: Any,
        **kwargs: Any,
    ) -> Future[T]:
        raise NotImplementedError
