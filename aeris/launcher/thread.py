from __future__ import annotations

import dataclasses
import sys
import threading
from types import TracebackType
from typing import Any
from typing import Generic
from typing import TypeVar

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from aeris.agent import Agent
from aeris.behavior import Behavior
from aeris.exchange import Exchange
from aeris.handle import Handle
from aeris.identifier import AgentIdentifier

BehaviorT = TypeVar('BehaviorT', bound=Behavior)


@dataclasses.dataclass
class _RunningAgent(Generic[BehaviorT]):
    agent: Agent[BehaviorT]
    thread: threading.Thread


class ThreadLauncher:
    """Local thread launcher.

    Launch agents in threads within the current process. This launcher is
    useful for local testing as the GIL will limit the performance and
    scalability of agents.
    """

    def __init__(self, exchange: Exchange) -> None:
        self._agents: dict[AgentIdentifier, _RunningAgent[Any]] = {}
        self._exchange = exchange

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        self.close()

    def __repr__(self) -> str:
        return f'{type(self).__name__}(exchange={self._exchange!r})'

    def __str__(self) -> str:
        name = type(self).__name__
        return f'{name}<{self._exchange}; {len(self._agents)} agents>'

    def close(self) -> None:
        """Close the launcher and shutdown agents."""
        for aid in self._agents:
            self._agents[aid].agent.shutdown()
        for aid in self._agents:
            self._agents[aid].thread.join()

    def launch(self, behavior: Behavior) -> Handle:
        """Launch a new agent with a specified behavior.

        Args:
            behavior: Behavior the agent should implement.

        Returns:
            Mailbox used to communicate with agent.
        """
        aid = self._exchange.register_agent()

        agent = Agent(behavior, aid=aid, exchange=self._exchange)
        thread = threading.Thread(target=agent)
        thread.start()
        self._agents[aid] = _RunningAgent(agent, thread)

        return self._exchange.create_handle(aid)
