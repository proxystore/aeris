from __future__ import annotations

import dataclasses
import logging
import sys
from concurrent.futures import Executor
from concurrent.futures import Future
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
from aeris.handle import RemoteHandle
from aeris.identifier import AgentIdentifier

logger = logging.getLogger(__name__)

BehaviorT = TypeVar('BehaviorT', bound=Behavior)


@dataclasses.dataclass
class _RunningAgent(Generic[BehaviorT]):
    agent: Agent[BehaviorT]
    future: Future[None]


class ExecutorLauncher:
    """Launcher that wraps a [`concurrent.futures.Executor`][concurrent.futures.Executor].

    Args:
        exchange: Exchange used for communication.
        executor: Executor used for launching agents. Note that this class
            takes ownership of the `executor`.
    """  # noqa: E501

    def __init__(self, exchange: Exchange, executor: Executor) -> None:
        self._exchange = exchange
        self._executor = executor
        self._agents: dict[AgentIdentifier, _RunningAgent[Any]] = {}

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
        return (
            f'{type(self).__name__}'
            f'(exchange={self._exchange!r}, executor={self._executor!r})'
        )

    def __str__(self) -> str:
        return f'{type(self).__name__}<{self._exchange}; {self._executor}>'

    def close(self) -> None:
        """Close the launcher and shutdown agents."""
        logger.debug('Waiting for all agents to shutdown...')
        self._executor.shutdown(wait=True)
        logger.info('Closed %s', self)

    def launch(self, behavior: BehaviorT) -> RemoteHandle[BehaviorT]:
        """Launch a new agent with a specified behavior.

        Args:
            behavior: Behavior the agent should implement.

        Returns:
            Mailbox used to communicate with agent.
        """
        aid = self._exchange.create_agent()

        agent = Agent(behavior, aid=aid, exchange=self._exchange)
        future = self._executor.submit(agent)
        self._agents[aid] = _RunningAgent(agent, future)
        logger.info('Launched %s', agent)

        return self._exchange.create_handle(aid)
