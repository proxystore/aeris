from __future__ import annotations

from typing import Any
from typing import Protocol

from aeris.behavior import Behavior
from aeris.exchange import Exchange
from aeris.exchange import Mailbox

__all__ = ['Launcher']


class Launcher(Protocol):
    def start(self, behavior: Behavior) -> Mailbox: ...
