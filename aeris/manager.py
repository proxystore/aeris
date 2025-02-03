from __future__ import annotations

import logging
import sys
from types import TracebackType
from typing import TypeVar

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from aeris.behavior import Behavior
from aeris.exchange import Exchange
from aeris.handle import RemoteHandle
from aeris.identifier import AgentIdentifier
from aeris.launcher import Launcher

logger = logging.getLogger(__name__)

BehaviorT = TypeVar('BehaviorT', bound=Behavior)


class Manager:
    """Launch and manage running agents.

    The manager is provided as convenience to reduce common boilerplate code
    for spawning agents and managing handles.

    Tip:
        This class can be used as a context manager. Upon exiting the context,
        running agents will be shutdown, any agent handles created by the
        manager will be closed, and the exchange and launcher will be closed.

    Note:
        The manager takes ownership of the exchange and launcher interfaces.
        This means the manager will be responsible for closing them once the
        manager is closed.

    Args:
        exchange: Exchange that agents and clients will use for communication.
        launcher: Launcher used to execute agents remotely.
    """

    def __init__(self, exchange: Exchange, launcher: Launcher) -> None:
        self._exchange = exchange
        self._launcher = launcher

        self._agents: dict[AgentIdentifier, RemoteHandle[Behavior]] = {}

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
            f'(exchange={self._exchange!r}, launcher={self._launcher!r})'
        )

    def __str__(self) -> str:
        return f'{type(self).__name__}<{self._exchange}, {self._launcher}>'

    @property
    def exchange(self) -> Exchange:
        """Exchange interface."""
        return self._exchange

    @property
    def launcher(self) -> Launcher:
        """Launcher interface."""
        return self._launcher

    def close(self) -> None:
        """Close the manager and cleanup resources."""
        for handle in self._agents.values():
            handle.shutdown()
            handle.close()
        self.exchange.close()
        self.launcher.close()

    def launch(self, behavior: BehaviorT) -> RemoteHandle[BehaviorT]:
        """Launch a new agent with a specified behavior.

        Note:
            Compared to `Launcher.launch()`, this method will inject the
            exchange and return a client-bound handle.

        Args:
            behavior: Behavior the agent should implement.

        Returns:
            Handle (client bound) used to interact with the agent.
        """
        handle = self.launcher.launch(behavior, exchange=self.exchange)
        client = handle.bind_as_client()
        self._agents[client.aid] = client
        return client
