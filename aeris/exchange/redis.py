from __future__ import annotations

import sys
from types import TracebackType
from typing import Any
from typing import get_args

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

try:
    import redis
except ImportError as e:  # pragma: no cover
    raise ImportError(
        'Unable to import redis. Did you install using aeris[redis]?',
    ) from e

from aeris.exception import BadIdentifierError
from aeris.exception import MailboxClosedError
from aeris.handle import Handle
from aeris.identifier import AgentIdentifier
from aeris.identifier import ClientIdentifier
from aeris.identifier import Identifier
from aeris.message import BaseMessage
from aeris.message import Message


class RedisMailbox:
    """Interface for a mailbox in a Redis-hosted message exchange.

    Args:
        uid: Identifier of this mailbox.
        client: Redis client. Note that this interface only borrows
            `client` and will not close `client` when the interface is
            closed.
        timeout: Timeout for waiting on the next message. If `None`, the
            timeout will be set to one second but will loop indefinitely.
    """

    def __init__(
        self,
        uid: Identifier,
        client: redis.Redis,
        *,
        timeout: float | None = None,
    ) -> None:
        self.uid = uid
        self.timeout = timeout
        self._queue_key = str(uid.uid)
        self._alive_key = f'{self._queue_key}-alive'
        self._client = client
        self._closed = False
        self._client.set(self._alive_key, True)

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        self.close()

    def send(self, message: Message) -> None:
        """Send a message to this mailbox.

        Raises:
            MailboxClosedError: if [`close()`][aeris.exchange.Mailbox.close]
                has been called.
        """
        if self._closed:
            raise MailboxClosedError
        self._client.rpush(self._queue_key, message.model_dump_json())

    def recv(self) -> Message:
        """Get the next message from this mailbox.

        Raises:
            MailboxClosedError: if [`close()`][aeris.exchange.Mailbox.close]
                has been called.
        """
        timeout = self.timeout if self.timeout is not None else 1
        while True:
            if self._closed or not bool(self._client.exists(self._alive_key)):
                raise MailboxClosedError

            raw = self._client.blpop(self._queue_key, timeout=timeout)
            if raw is None and self.timeout is not None:
                raise TimeoutError(
                    f'Timeout waiting for next message for {self.uid!r} after '
                    f'{self.timeout} seconds.',
                )
            elif raw is None:
                continue

            # Only passed one key to blpop to result is tuple(key, item)
            assert len(raw) == 2  # noqa: PLR2004
            message = BaseMessage.model_from_json(raw[1])
            assert isinstance(message, get_args(Message))
            return message

    def close(self) -> None:
        """Close the mailbox.

        This unregisters the entity from the exchange.
        """
        if not self._closed:
            self._closed = True
            self._client.delete(self._alive_key)


class RedisExchange:
    """Redis-hosted message exchange interface.

    hostname: Redis server hostname.
    port: Redis server port.
    kwargs: Extra keyword arguments to pass to
        [`redis.Redis()`][redis.Redis].
    timeout: Timeout for waiting on the next message. If `None`, the
        timeout will be set to one second but will loop indefinitely.
    """

    def __init__(
        self,
        hostname: str,
        port: int,
        *,
        timeout: float | None = None,
        **kwargs: Any,
    ) -> None:
        self.hostname = hostname
        self.port = port
        self.timeout = timeout
        self._kwargs = kwargs
        self._client = redis.Redis(hostname=hostname, port=port, **kwargs)
        self._mailboxes: dict[Identifier, RedisMailbox] = {}

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        self.close()

    def __getstate__(self) -> dict[str, Any]:
        return {
            'hostname': self.hostname,
            'port': self.port,
            'timeout': self.timeout,
            '_kwargs': self._kwargs,
        }

    def __setstate__(self, state: dict[str, Any]) -> None:
        self.__dict__.update(state)
        self._client = redis.Redis(
            hostname=self.hostname,
            port=self.port,
            **self._kwargs,
        )
        self._mailboxes = {}

    def __repr__(self) -> str:
        return f'{type(self).__name__}("{self.hostname}:{self.port}")'

    def __str__(self) -> str:
        return f'{type(self).__name__}<{self.hostname}:{self.port}>'

    def close(self) -> None:
        """Close the exchange interface."""
        self._client.close()

    def register_agent(self, name: str | None = None) -> AgentIdentifier:
        """Create a mailbox for a new agent in the system.

        Args:
            name: Optional human-readable name for the agent.
        """
        aid = AgentIdentifier.new(name=name)
        self._mailboxes[aid] = RedisMailbox(
            aid,
            self._client,
            timeout=self.timeout,
        )
        return aid

    def register_client(self, name: str | None = None) -> ClientIdentifier:
        """Create a mailbox for a new client in the system.

        Args:
            name: Optional human-readable name for the client.
        """
        cid = ClientIdentifier.new(name=name)
        self._mailboxes[cid] = RedisMailbox(
            cid,
            self._client,
            timeout=self.timeout,
        )
        return cid

    def unregister(self, uid: Identifier) -> None:
        """Unregister the entity (either agent or client).

        Args:
            uid: Identifier of the entity to unregister.
        """
        mailbox = self._mailboxes.pop(uid, None)
        if mailbox is not None:
            mailbox.close()

    def create_handle(self, aid: AgentIdentifier) -> Handle:
        """Create a handle to an agent in the system.

        A handle enables a client to invoke actions on the agent.

        Note:
            It is not possible to create a handle to a client since a handle
            is essentially a new client of a specific agent.

        Args:
            aid: Identifier of agent in the system to create a handle to.

        Returns:
            Handle to the agent.

        Raises:
            TypeError: if `aid` is not an instance of
                [`AgentIdentifier`][aeris.identifier.AgentIdentifier].
        """
        if not isinstance(aid, AgentIdentifier):
            raise TypeError(
                f'Handle must be created from an {AgentIdentifier.__name__} '
                f'but got identifier with type {type(aid).__name__}.',
            )
        return Handle(aid, self)

    def get_mailbox(self, uid: Identifier) -> RedisMailbox:
        """Get the mailbox for an entity in the system.

        Args:
            uid: Identifier of entity in the system.

        Returns:
            Mailbox for the entity.

        Raises:
            BadIdentifierError: if an entity with `uid` is not
                registered with the exchange.
        """
        try:
            return self._mailboxes[uid]
        except KeyError as e:
            raise BadIdentifierError(
                f'{uid} is not registered with this exchange.',
            ) from e
