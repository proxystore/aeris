from __future__ import annotations

from typing import Any
from typing import Protocol

from aeris.behavior import Behavior
from aeris.exchange import Exchange
from aeris.exchange import Mailbox

__all__ = ['Launcher']


class Launcher(Protocol):
    """Agent launcher protocol.

    A launcher manages the create and execution of agents on remote resources.
    """

    def launch(self, behavior: Behavior) -> Mailbox:
        """Launch a new agent with a specified behavior.

        Args:
            behavior: Behavior the agent should implement.

        Returns:
            Mailbox used to communicate with agent.
        """
        ...

    def shutdown(self) -> None:
        """Shutdown the launcher and agents."""
        ...
