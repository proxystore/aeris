from __future__ import annotations

import logging
import sys
import threading
from types import TracebackType
from typing import TypeVar

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from aeris.behavior import Behavior
from aeris.exchange import Exchange
from aeris.handle import RemoteHandle
from aeris.identifier import ClientIdentifier
from aeris.launcher import Launcher
from aeris.message import RequestMessage
from aeris.multiplex import MailboxMultiplexer

logger = logging.getLogger(__name__)

BehaviorT = TypeVar('BehaviorT', bound=Behavior)


class Manager:
    """Launch and manage running agents.

    The manager is provided as convenience to reduce common boilerplate code
    for spawning agents and managing handles. Each manager registers itself
    as a client in the exchange (i.e., each manager has its own mailbox).
    Handles created by the manager are bound to this mailbox.

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

        self._mailbox_id = exchange.create_client()
        self._multiplexer = MailboxMultiplexer(
            self.mailbox_id,
            self._exchange,
            self._handle_request,
        )
        self._listener_thread = threading.Thread(
            target=self._multiplexer.listen,
            name=f'multiplexer-{self.mailbox_id.uid}-listener',
        )
        self._listener_thread.start()

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
        return (
            f'{type(self).__name__}<{self.mailbox_id}, {self._exchange}, '
            f'{self._launcher}>'
        )

    @property
    def exchange(self) -> Exchange:
        """Exchange interface."""
        return self._exchange

    @property
    def launcher(self) -> Launcher:
        """Launcher interface."""
        return self._launcher

    @property
    def mailbox_id(self) -> ClientIdentifier:
        """Identifier of the mailbox used by this manager."""
        return self._mailbox_id

    def _handle_request(self, request: RequestMessage) -> None:
        response = request.error(
            TypeError(
                f'Client with {self.mailbox_id} cannot fulfill requests.',
            ),
        )
        self.exchange.send(response.dest, response)

    def close(self) -> None:
        """Close the manager and cleanup resources.

        1. Call shutdown on all launched agents.
        1. Close all handles created by the manager.
        1. Close the mailbox associated with the manager.
        1. Close the exchange.
        1. Close the launcher.
        """
        for handle in self._multiplexer.bound_handles.values():
            handle.shutdown()
        self._multiplexer.close_bound_handles()
        self._multiplexer.close_mailbox()
        self._listener_thread.join()
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
        unbound = self.launcher.launch(behavior, exchange=self.exchange)
        bound = self._multiplexer.bind(unbound)
        return bound
