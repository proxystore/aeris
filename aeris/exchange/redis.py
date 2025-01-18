from __future__ import annotations

import logging
from typing import Any
from typing import get_args

try:
    import redis
except ImportError as e:  # pragma: no cover
    raise ImportError(
        'Unable to import redis. Did you install using aeris[redis]?',
    ) from e

from aeris.exception import BadIdentifierError
from aeris.exception import MailboxClosedError
from aeris.exchange import ExchangeMixin
from aeris.identifier import Identifier
from aeris.message import BaseMessage
from aeris.message import Message

logger = logging.getLogger(__name__)


class RedisExchange(ExchangeMixin):
    """Redis-hosted message exchange interface.

    Args:
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
        self._client = redis.Redis(host=hostname, port=port, **kwargs)

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

    def __repr__(self) -> str:
        return f'{type(self).__name__}("{self.hostname}:{self.port}")'

    def __str__(self) -> str:
        return f'{type(self).__name__}<{self.hostname}:{self.port}>'

    def _active_key(self, uid: Identifier) -> str:
        return f'{uid.uid}-active'

    def _queue_key(self, uid: Identifier) -> str:
        return f'{uid.uid}-queue'

    def close(self) -> None:
        """Close the exchange interface."""
        self._client.close()

    def create_mailbox(self, uid: Identifier) -> None:
        """Create the mailbox in the exchange for a new entity.

        Note:
            This method is a no-op if the mailbox already exists.

        Args:
            uid: Entity identifier used as the mailbox address.
        """
        self._client.set(self._active_key(uid), True)
        logger.info(f'{self} created mailbox for {uid}')

    def close_mailbox(self, uid: Identifier) -> None:
        """Close the mailbox for an entity from the exchange.

        Note:
            This method is a no-op if the mailbox does not exist.

        Args:
            uid: Entity identifier of the mailbox to close.
        """
        self._client.set(self._active_key(uid), False)
        self._client.delete(self._queue_key(uid))
        logger.info(f'{self} closed mailbox for {uid}')

    def send(self, uid: Identifier, message: Message) -> None:
        """Send a message to a mailbox.

        Args:
            uid: Destination address of the message.
            message: Message to send.

        Raises:
            BadIdentifierError: if a mailbox for `uid` does not exist.
            MailboxClosedError: if the mailbox was closed.
        """
        status = self._client.get(self._active_key(uid))
        if status is None:
            raise BadIdentifierError()
        if status:
            self._client.rpush(self._queue_key(uid), message.model_dump_json())
        else:
            raise MailboxClosedError()

    def recv(self, uid: Identifier) -> Message:
        """Receive the next message addressed to an entity.

        Args:
            uid: Identifier of the entity requesting it's next message.

        Returns:
            Next message in the entity's mailbox.

        Raises:
            BadIdentifierError: if a mailbox for `uid` does not exist.
            MailboxClosedError: if the mailbox was closed.
        """
        timeout = self.timeout if self.timeout is not None else 1
        while True:
            status = self._client.get(self._active_key(uid))
            if status is None:
                raise BadIdentifierError()
            if not status:
                raise MailboxClosedError()

            raw = self._client.blpop(self._queue_key(uid), timeout=timeout)
            if raw is None and self.timeout is not None:
                raise TimeoutError(
                    f'Timeout waiting for next message for {uid!r} after '
                    f'{self.timeout} seconds.',
                )
            elif raw is None:
                continue

            # Only passed one key to blpop to result is tuple(key, item)
            assert len(raw) == 2  # noqa: PLR2004
            message = BaseMessage.model_from_json(raw[1])
            assert isinstance(message, get_args(Message))
            return message
