from __future__ import annotations

from typing import Any
from typing import Protocol
from typing import runtime_checkable

from aeris.behavior import Behavior
from aeris.exchange import Exchange
from aeris.exchange import Mailbox
from aeris.handle import Handle

__all__ = ['Launcher']


@runtime_checkable
class Launcher(Protocol):
    """Agent launcher protocol.

    A launcher manages the create and execution of agents on remote resources.
    """

    def launch(self, behavior: Behavior) -> Handle:
        """Launch a new agent with a specified behavior.

        Args:
            behavior: Behavior the agent should implement.

        Returns:
            Handle to the agent.
        """
        ...

    def shutdown(self) -> None:
        """Shutdown the launcher and agents."""
        ...
