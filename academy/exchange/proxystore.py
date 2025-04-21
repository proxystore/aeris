from __future__ import annotations

import functools
from collections.abc import Iterable
from collections.abc import Mapping
from typing import Any
from typing import Callable
from typing import TypeVar

from proxystore.proxy import Proxy
from proxystore.store import get_or_create_store
from proxystore.store import register_store
from proxystore.store import Store
from proxystore.store.utils import resolve_async

from academy.behavior import Behavior
from academy.exchange import Exchange
from academy.exchange import ExchangeMixin
from academy.exchange import Mailbox
from academy.identifier import AgentId
from academy.identifier import ClientId
from academy.identifier import EntityId
from academy.message import ActionRequest
from academy.message import ActionResponse
from academy.message import Message
from academy.serialize import NoPickleMixin

BehaviorT = TypeVar('BehaviorT', bound=Behavior)


def _proxy_item(
    item: Any,
    store: Store[Any],
    should_proxy: Callable[[Any], bool],
) -> Any:
    if type(item) is not Proxy and should_proxy(item):
        return store.proxy(item)
    return item


def _proxy_iterable(
    items: Iterable[Any],
    store: Store[Any],
    should_proxy: Callable[[Any], bool],
) -> tuple[Any, ...]:
    _apply = functools.partial(
        _proxy_item,
        store=store,
        should_proxy=should_proxy,
    )
    return tuple(map(_apply, items))


def _proxy_mapping(
    mapping: Mapping[Any, Any],
    store: Store[Any],
    should_proxy: Callable[[Any], bool],
) -> dict[Any, Any]:
    _apply = functools.partial(
        _proxy_item,
        store=store,
        should_proxy=should_proxy,
    )
    return {key: _apply(item) for key, item in mapping.items()}


class ProxyStoreExchange(ExchangeMixin):
    """Wrap an Exchange with ProxyStore support.

    Sending large action payloads via the exchange can result in considerable
    slowdowns. This Exchange wrapper can replace arguments in action requests
    and results in action responses with proxies to reduce communication
    costs.

    Args:
        exchange: Exchange to wrap.
        store: Store to use for proxying data.
        should_proxy: A callable that returns `True` if an object should be
            proxied. This is applied to every positional and keyword argument
            and result value.
        resolve_async: Resolve proxies asynchronously when received.
    """

    def __init__(
        self,
        exchange: Exchange,
        store: Store[Any],
        should_proxy: Callable[[Any], bool],
        *,
        resolve_async: bool = False,
    ) -> None:
        self.exchange = exchange
        self.store = store
        self.should_proxy = should_proxy
        self.resolve_async = resolve_async
        register_store(store, exist_ok=True)

    def __getstate__(self) -> dict[str, Any]:
        return {
            'exchange': self.exchange,
            'store_config': self.store.config(),
            'resolve_async': self.resolve_async,
            'should_proxy': self.should_proxy,
        }

    def __setstate__(self, state: dict[str, Any]) -> None:
        self.store = get_or_create_store(
            state.pop('store_config'),
            register=True,
        )
        self.__dict__.update(state)
        register_store(self.store, exist_ok=True)

    def close(self) -> None:
        """Close the exchange client.

        Note:
            This does not alter the state of the exchange.
        """
        self.exchange.close()

    def register_agent(
        self,
        behavior: type[BehaviorT],
        *,
        agent_id: AgentId[BehaviorT] | None = None,
        name: str | None = None,
    ) -> AgentId[BehaviorT]:
        """Create a new agent identifier and associated mailbox.

        Args:
            behavior: Type of the behavior this agent will implement.
            agent_id: Specify the ID of the agent. Randomly generated
                default.
            name: Optional human-readable name for the agent. Ignored if
                `agent_id` is provided.

        Returns:
            Unique identifier for the agent's mailbox.
        """
        return self.exchange.register_agent(
            behavior,
            agent_id=agent_id,
            name=name,
        )

    def register_client(
        self,
        *,
        name: str | None = None,
    ) -> ClientId:
        """Create a new client identifier and associated mailbox.

        Args:
            name: Optional human-readable name for the client.

        Returns:
            Unique identifier for the client's mailbox.
        """
        return self.exchange.register_client(name=name)

    def terminate(self, uid: EntityId) -> None:
        """Close the mailbox for an entity from the exchange.

        Note:
            This method is a no-op if the mailbox does not exist.

        Args:
            uid: Entity identifier of the mailbox to close.
        """
        self.exchange.terminate(uid)

    def discover(
        self,
        behavior: type[Behavior],
        *,
        allow_subclasses: bool = True,
    ) -> tuple[AgentId[Any], ...]:
        """Discover peer agents with a given behavior.

        Args:
            behavior: Behavior type of interest.
            allow_subclasses: Return agents implementing subclasses of the
                behavior.

        Returns:
            Tuple of agent IDs implementing the behavior.
        """
        return self.exchange.discover(
            behavior,
            allow_subclasses=allow_subclasses,
        )

    def get_mailbox(self, uid: EntityId) -> Mailbox:
        """Get a client to a specific mailbox.

        Args:
            uid: EntityId of the mailbox.

        Returns:
            Mailbox client.

        Raises:
            BadEntityIdError: if a mailbox for `uid` does not exist.
        """
        base_mailbox = self.exchange.get_mailbox(uid)
        return ProxyStoreMailbox(base_mailbox, self, self.resolve_async)

    def send(self, uid: EntityId, message: Message) -> None:
        """Send a message to a mailbox.

        Args:
            uid: Destination address of the message.
            message: Message to send.

        Raises:
            BadEntityIdError: if a mailbox for `uid` does not exist.
            MailboxClosedError: if the mailbox was closed.
        """
        if isinstance(message, ActionRequest):
            message.args = _proxy_iterable(
                message.args,
                self.store,
                self.should_proxy,
            )
            message.kwargs = _proxy_mapping(
                message.kwargs,
                self.store,
                self.should_proxy,
            )
        if isinstance(message, ActionResponse) and message.result is not None:
            message.result = _proxy_item(
                message.result,
                self.store,
                self.should_proxy,
            )

        self.exchange.send(uid, message)


class ProxyStoreMailbox(NoPickleMixin):
    """Client protocol that listens to incoming messages to a mailbox.

    Args:
        mailbox: The mailbox created by the wrapped exchange.
        exchange: The wrapper exchange.
        resolve_async: Begin resolving proxies in action requests or responses
            asynchronously once the message is received.
    """

    def __init__(
        self,
        mailbox: Mailbox,
        exchange: ProxyStoreExchange,
        resolve_async: bool = False,
    ) -> None:
        self._exchange = exchange
        self._mailbox = mailbox
        self._resolve_async = resolve_async

    @property
    def exchange(self) -> Exchange:
        """Exchange client."""
        return self._exchange

    @property
    def mailbox_id(self) -> EntityId:
        """Mailbox address/identifier."""
        return self._mailbox.mailbox_id

    def close(self) -> None:
        """Close this mailbox client.

        Warning:
            This does not close the mailbox in the exchange. I.e., the exchange
            will still accept new messages to this mailbox, but this client
            will no longer be listening for them.
        """
        self._mailbox.close()

    def recv(self, timeout: float | None = None) -> Message:
        """Receive the next message in the mailbox.

        This blocks until the next message is received or the mailbox
        is closed.

        Args:
            timeout: Optional timeout in seconds to wait for the next
                message. If `None`, the default, block forever until the
                next message or the mailbox is closed.

        Raises:
            MailboxClosedError: if the mailbox was closed.
            TimeoutError: if a `timeout` was specified and exceeded.
        """
        message = self._mailbox.recv(timeout)
        if self._resolve_async and isinstance(message, ActionRequest):
            for arg in (*message.args, *message.kwargs.values()):
                if type(arg) is Proxy:
                    resolve_async(arg)
        elif (
            self._resolve_async
            and isinstance(message, ActionResponse)
            and type(message.result) is Proxy
        ):
            resolve_async(message.result)
        return message
