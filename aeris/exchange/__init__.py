from __future__ import annotations

import sys
from concurrent.futures import Future
from typing import Any
from typing import Protocol
from typing import runtime_checkable
from typing import TypeVar

from aeris.handle import Handle
from aeris.identifier import AgentIdentifier
from aeris.identifier import ClientIdentifier
from aeris.identifier import Identifier

T = TypeVar('T')

__all__ = ['Exchange', 'Mailbox']


@runtime_checkable
class Mailbox(Protocol):
    def send(self, message: Any) -> None: ...

    def recv(self) -> Any: ...


@runtime_checkable
class Exchange(Protocol):
    def register_agent(self) -> AgentIdentifier: ...

    def register_client(self) -> ClientIdentifier: ...

    def create_handle(self, uid: AgentIdentifier) -> Handle: ...

    def get_mailbox(self, uid: Identifier) -> Mailbox | None: ...
