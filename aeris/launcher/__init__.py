from __future__ import annotations

from typing import Protocol
from typing import runtime_checkable
from typing import TypeVar

from aeris.behavior import Behavior
from aeris.handle import RemoteHandle

__all__ = ['Launcher']

BehaviorT = TypeVar('BehaviorT', bound=Behavior)


@runtime_checkable
class Launcher(Protocol):
    """Agent launcher protocol.

    A launcher manages the create and execution of agents on remote resources.
    """

    def close(self) -> None:
        """Close the launcher and shutdown agents."""
        ...

    def launch(self, behavior: BehaviorT) -> RemoteHandle[BehaviorT]:
        """Launch a new agent with a specified behavior.

        Args:
            behavior: Behavior the agent should implement.

        Returns:
            Handle to the agent.
        """
        ...
