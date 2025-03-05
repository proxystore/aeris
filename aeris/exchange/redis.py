from __future__ import annotations

import enum
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


class _MailboxState(enum.Enum):
    ACTIVE = 'ACTIVE'
    INACTIVE = 'INACTIVE'


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
        timeout: int | None = None,
        **kwargs: Any,
    ) -> None:
        self.hostname = hostname
        self.port = port
        self.timeout = timeout
        self._kwargs = kwargs
        self._client = redis.Redis(
            host=hostname,
            port=port,
            decode_responses=True,
            **kwargs,
        )

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
            host=self.hostname,
            port=self.port,
            decode_responses=True,
            **self._kwargs,
        )

    def __repr__(self) -> str:
        return (
            f'{type(self).__name__}(hostname={self.hostname}, '
            f'port={self.port}, timeout={self.timeout})'
        )

    def __str__(self) -> str:
        return f'{type(self).__name__}<{self.hostname}:{self.port}>'

    def _active_key(self, uid: Identifier) -> str:
        return f'{uid.uid}-active'

    def _queue_key(self, uid: Identifier) -> str:
        return f'{uid.uid}-queue'

    def close(self) -> None:
        """Close the exchange interface."""
        self._client.close()
        logger.debug('Closed exchange (%s)', self)

    def create_mailbox(self, uid: Identifier) -> None:
        """Create the mailbox in the exchange for a new entity.

        Note:
            This method is a no-op if the mailbox already exists.

        Args:
            uid: Entity identifier used as the mailbox address.
        """
        self._client.set(self._active_key(uid), _MailboxState.ACTIVE.value)
        logger.debug('Created mailbox for %s (%s)', uid, self)

    def close_mailbox(self, uid: Identifier) -> None:
        """Close the mailbox for an entity from the exchange.

        Note:
            This method is a no-op if the mailbox does not exist.

        Args:
            uid: Entity identifier of the mailbox to close.
        """
        self._client.set(self._active_key(uid), _MailboxState.INACTIVE.value)
        self._client.delete(self._queue_key(uid))
        logger.debug('Closed mailbox for %s (%s)', uid, self)

    def get_mailbox(self, uid: Identifier) -> RedisMailbox:
        """Get a client to a specific mailbox.

        Args:
            uid: Identifier of the mailbox.

        Returns:
            Mailbox client.

        Raises:
            BadIdentifierError: if a mailbox for `uid` does not exist.
        """
        return RedisMailbox(uid, self)

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
            raise BadIdentifierError(uid)
        elif status == _MailboxState.INACTIVE.value:
            raise MailboxClosedError(uid)
        else:
            self._client.rpush(self._queue_key(uid), message.model_dump_json())
            logger.debug('Sent %s to %s', type(message).__name__, uid)


class RedisMailbox:
    """Client protocol that listens to incoming messages to a mailbox.

    Args:
        uid: Identifier of the mailbox.
        exchange: Exchange client.

    Raises:
        BadIdentifierError: if a mailbox with `uid` does not exist.
    """

    def __init__(self, uid: Identifier, exchange: RedisExchange) -> None:
        self._uid = uid
        self._exchange = exchange

        status = self.exchange._client.get(self.exchange._active_key(uid))
        if status is None:
            raise BadIdentifierError(uid)

    @property
    def exchange(self) -> RedisExchange:
        """Exchange client."""
        return self._exchange

    @property
    def mailbox_id(self) -> Identifier:
        """Mailbox address/identifier."""
        return self._uid

    def close(self) -> None:
        """Close this mailbox client.

        Warning:
            This does not close the mailbox in the exchange. I.e., the exchange
            will still accept new messages to this mailbox, but this client
            will no longer be listening for them.
        """
        pass

    def recv(self, timeout: float | None = None) -> Message:
        """Receive the next message in the mailbox.

        This blocks until the next message is received or the mailbox
        is closed.

        Args:
            timeout: Optional timeout in seconds to wait for the next
                message. If `None`, the default, block forever until the
                next message or the mailbox is closed. Note that this will
                be cast to an int which is required by the Redis API.

        Raises:
            MailboxClosedError: if the mailbox was closed.
            TimeoutError: if a `timeout` was specified and exceeded.
        """
        while True:
            status = self.exchange._client.get(
                self.exchange._active_key(self.mailbox_id),
            )
            if status is None:
                raise AssertionError(
                    f'Status for mailbox {self.mailbox_id} did not exist in '
                    'Redis server. This means that something incorrectly '
                    'deleted the key.',
                )
            elif status == _MailboxState.INACTIVE.value:
                raise MailboxClosedError(self.mailbox_id)

            raw = self.exchange._client.blpop(
                [self.exchange._queue_key(self.mailbox_id)],
                timeout=int(timeout) if timeout is not None else None,
            )
            if raw is None and timeout is not None:
                raise TimeoutError(
                    f'Timeout waiting for next message for {self.mailbox_id} '
                    f'after {timeout} seconds.',
                )
            elif raw is None:  # pragma: no cover
                continue

            # Only passed one key to blpop to result is [key, item]
            assert isinstance(raw, (tuple, list))
            assert len(raw) == 2  # noqa: PLR2004
            message = BaseMessage.model_from_json(raw[1])
            assert isinstance(message, get_args(Message))
            logger.debug(
                'Received %s to %s',
                type(message).__name__,
                self.mailbox_id,
            )
            return message
