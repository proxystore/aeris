from __future__ import annotations

import queue
from typing import Any

from aeris.handle import Handle
from aeris.identifier import AgentIdentifier
from aeris.identifier import ClientIdentifier
from aeris.identifier import Identifier


class ThreadMailbox:
    def __init__(self) -> None:
        self._mail: queue.Queue[Any] = queue.Queue()

    def send(self, message: Any) -> None:
        self._mail.put(message)

    def recv(self) -> Any:
        return self._mail.get(block=True)


class ThreadExchange:
    def __init__(self) -> None:
        self.mailboxes: dict[Identifier, ThreadMailbox] = {}

    def register_agent(self) -> AgentIdentifier:
        aid = AgentIdentifier.new()
        self.mailboxes[aid] = ThreadMailbox()
        return aid

    def register_client(self) -> ClientIdentifier:
        cid = ClientIdentifier.new()
        self.mailboxes[cid] = ThreadMailbox()
        return cid

    def create_handle(self, aid: AgentIdentifier) -> Handle:
        if not isinstance(aid, AgentIdentifier):
            raise TypeError(
                f'Handle must be created from an {AgentIdentifier.__name__} '
                f'but got identifier with type {type(aid).__name__}.',
            )
        return Handle(aid, self)

    def get_mailbox(self, uid: Identifier) -> ThreadMailbox | None:
        return self.mailboxes.get(uid)
