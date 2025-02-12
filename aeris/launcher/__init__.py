from __future__ import annotations

from typing import Protocol
from typing import runtime_checkable
from typing import TypeVar

from aeris.behavior import Behavior
from aeris.exchange import Exchange
from aeris.handle import RemoteHandle
from aeris.identifier import AgentIdentifier

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

    def launch(
        self,
        behavior: BehaviorT,
        exchange: Exchange,
        *,
        agent_id: AgentIdentifier | None = None,
    ) -> RemoteHandle[BehaviorT]:
        """Launch a new agent with a specified behavior.

        Args:
            behavior: Behavior the agent should implement.
            exchange: Exchange the agent will use for messaging.
            agent_id: Specify ID of the launched agent. If `None`, a new
                agent ID will be created within the exchange.

        Returns:
            Handle to the agent.
        """
        ...
