from __future__ import annotations

import concurrent.futures
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
from aeris.exception import BadIdentifierError
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
        self._future_to_id: dict[Future[None], AgentIdentifier] = {}
        self._id_to_future: dict[AgentIdentifier, Future[None]] = {}

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
        return f'{type(self).__name__}<{type(self._executor).__name__}>'

    def _callback(self, future: Future[None]) -> None:
        agent_id = self._future_to_id[future]
        try:
            future.result()
            logger.debug('Completed agent future (%s)', agent_id)
        except CancelledError:  # pragma: no cover
            logger.warning('Cancelled agent future (%s)', agent_id)
        except Exception:  # pragma: no cover
            logger.exception('Received agent exception (%s)', agent_id)

    def close(self) -> None:
        """Close the launcher and shutdown agents."""
        logger.debug('Waiting for agents to shutdown...')
        for future in self._future_to_id.copy():
            future.result()
        self._executor.shutdown(wait=True, cancel_futures=True)
        logger.debug('Closed launcher (%s)', self)

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
            Handle (unbound) used to interact with the agent.
        """
        agent_id = exchange.create_agent() if agent_id is None else agent_id

        agent = Agent(
            behavior,
            agent_id=agent_id,
            exchange=exchange,
            close_exchange=True,
        )
        future = self._executor.submit(agent)
        future.add_done_callback(self._callback)
        self._future_to_id[future] = agent_id
        self._id_to_future[agent_id] = future
        logger.debug('Launched agent (%s; %s)', agent_id, behavior)

        return exchange.create_handle(agent_id)

    def running(self) -> set[AgentIdentifier]:
        """Get a set of IDs for all running agents.

        Returns:
            Set of agent IDs corresponding to all agents launched by this \
            launcher that have not completed yet.
        """
        running: set[AgentIdentifier] = set()
        for future, agent_id in self._future_to_id.items():
            if not future.done():
                running.add(agent_id)
        return running

    def wait(
        self,
        agent_id: AgentIdentifier,
        *,
        timeout: float | None = None,
    ) -> None:
        """Wait for a launched agent to exit.

        Args:
            agent_id: ID of launched agent.
            timeout: Optional timeout in seconds to wait for agent.

        Raises:
            BadIdentifierError: If an agent with `agent_id` was not
                launched by this launcher.
            TimeoutError: If `timeout` was exceeded while waiting for agent.
        """
        try:
            future = self._id_to_future[agent_id]
        except KeyError:
            raise BadIdentifierError(agent_id) from None

        if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
            try:
                future.result(timeout=timeout)
            except TimeoutError:
                raise
            except Exception:
                pass
        else:  # pragma: <3.11 cover
            try:
                future.result(timeout=timeout)
            except concurrent.futures.TimeoutError:
                raise TimeoutError() from None
            except Exception:
                pass
