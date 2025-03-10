from __future__ import annotations

import enum
import logging
from typing import Any
from typing import get_args

import redis

from aeris.exception import BadIdentifierError
from aeris.exception import MailboxClosedError
from aeris.exchange import ExchangeMixin
from aeris.identifier import Identifier
from aeris.message import BaseMessage
from aeris.message import Message

logger = logging.getLogger(__name__)


class RabbitMQExchange(ExchangeMixing):
    """RabbitMQ (AMQP) message exchange interface.
    """

    def __init__(
        self,
        host: str = 'localhost',
        port: int = 5672,
        **kwargs: Any,
    ) -> None:
        self._params = pika.ConnectionParameters(
            host=host,
            port=port,
            **kwargs,
        )
        self._connection = pika.BlockingConnection(self._params)
        self._channel = self._connection.channel()

    def __repr__(self) -> str:
        return (
            f'{type(self).__name__}(host={self._params.host}, '
            f'port={self._params.port})'
        )

    def __str__(self) -> str:
        return (
            f'{type(self).__name__}'
            f'<{self._params.host}:{self._params.port}>'
        )


    def close(self) -> None:
        """Close the exchange client.

        Note:
            This does not alter the state of the exchange.
        """
        self._channel.close()
        self._connection.close()

    def create_mailbox(self, uid: Identifier) -> None:
        """Create the mailbox in the exchange for a new entity.

        Note:
            This method is a no-op if the mailbox already exists.

        Args:
            uid: Entity identifier used as the mailbox address.
        """
        self._channel.queue_declare(queue=str(uid.uid))
        logger.debug('Created mailbox for %s (%s)', uid, self)

    def close_mailbox(self, uid: Identifier) -> None:
        """Close the mailbox for an entity from the exchange.

        Note:
            This method is a no-op if the mailbox does not exist.

        Args:
            uid: Entity identifier of the mailbox to close.
        """
        self._channel.queue_delete(queue=str(uid.uid))
        logger.debug('Closed mailbox for %s (%s)', uid, self)

    def send(self, uid: Identifier, message: Message) -> None:
        """Send a message to a mailbox.

        Args:
            uid: Destination address of the message.
            message: Message to send.

        Raises:
            BadIdentifierError: if a mailbox for `uid` does not exist.
            MailboxClosedError: if the mailbox was closed.
        """
        self._channel.basic_publish(
            exchange='',
            routing_key=str(uid.uid),
            body=message.model_dump_json(),
            mandatory=True,
        )

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
        self._channel.basic_consume(
            queue=
