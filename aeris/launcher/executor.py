from __future__ import annotations

import logging
import sys
from concurrent.futures import CancelledError
from concurrent.futures import Executor
from concurrent.futures import Future
from types import TracebackType
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


class ExecutorLauncher:
    """Launcher that wraps a [`concurrent.futures.Executor`][concurrent.futures.Executor].

    Args:
        executor: Executor used for launching agents. Note that this class
            takes ownership of the `executor`.
    """  # noqa: E501

    def __init__(self, executor: Executor) -> None:
        self._executor = executor
        self._futures: dict[Future[None], AgentIdentifier] = {}

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
        return f'{type(self).__name__}(executor={self._executor!r})'

    def __str__(self) -> str:
        return f'{type(self).__name__}<{self._executor}>'

    def _callback(self, future: Future[None]) -> None:
        aid = self._futures.pop(future)
        try:
            future.result()
            logger.info('Safely completed agent with %s', aid)
        except CancelledError:  # pragma: no cover
            logger.warning('Cancelled agent future with %s', aid)
        except Exception:  # pragma: no cover
            logger.exception('Runtime exception in agent with %s', aid)

    def close(self) -> None:
        """Close the launcher and shutdown agents."""
        logger.debug('Waiting for all agents to shutdown...')
        for fut in self._futures.copy():
            fut.result()
        self._executor.shutdown(wait=True, cancel_futures=True)
        logger.info('Closed %s', self)

    def launch(
        self,
        behavior: BehaviorT,
        exchange: Exchange,
    ) -> RemoteHandle[BehaviorT]:
        """Launch a new agent with a specified behavior.

        Args:
            behavior: Behavior the agent should implement.
            exchange: Exchange the agent will use for messaging.

        Returns:
            Mailbox used to communicate with agent.
        """
        aid = exchange.create_agent()

        agent = Agent(
            behavior,
            aid=aid,
            exchange=exchange,
            close_exchange=True,
        )
        future = self._executor.submit(agent)
        future.add_done_callback(self._callback)
        self._futures[future] = aid
        logger.info('Launched agent with %s', agent)

        return exchange.create_handle(aid)
