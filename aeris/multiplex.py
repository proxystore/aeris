from __future__ import annotations

import logging
import sys
import uuid
from types import TracebackType
from typing import Any
from typing import Callable
from typing import get_args
from typing import TypeVar

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from aeris.behavior import Behavior
from aeris.exception import MailboxClosedError
from aeris.exchange import Exchange
from aeris.handle import BoundRemoteHandle
from aeris.handle import RemoteHandle
from aeris.identifier import Identifier
from aeris.message import Message
from aeris.message import RequestMessage
from aeris.message import ResponseMessage
from aeris.serialize import NoPickleMixin

logger = logging.getLogger(__name__)

BehaviorT_co = TypeVar('BehaviorT_co', bound=Behavior, covariant=True)


class MailboxMultiplexer(NoPickleMixin):
    """Multiplex a single mailbox across many consumers.

    A mailbox represents a recipient entity. In many cases, there may be
    many entities within a single process that want to send and receive
    messages. For example, a running agent may have multiple handles to other
    agents. A naive approach would be for the agent and each handle to have
    their own mailbox, but this requires a listening thread in the process
    for each mailbox. This does not scale well. The multiplexer lets
    multiple entities (e.g., an agent and its handles) share the a single
    mailbox so their is one listening thread and messages are dispatched
    to the appropriate entity (i.e., object) within the process.

    Note:
        This class should not be considered as a part of the public API. It
        is used internally by other components, such as the
        [`Agent`][aeris.agent.Agent] and [`Manager`][aeris.manager.Manager],
        which use multiple handles concurrently.

    Args:
        mailbox_id: Identifier of the mailbox to multiplex. For example, the
            identifier of an agent.
        exchange: The exchange interface managing the mailbox.
        request_handler: A callable invoked when the request message is
            received to the inbox.
    """

    def __init__(
        self,
        mailbox_id: Identifier,
        exchange: Exchange,
        request_handler: Callable[[RequestMessage], None],
    ) -> None:
        self.mailbox_id = mailbox_id
        self.exchange = exchange
        self.request_handler = request_handler
        self.bound_handles: dict[uuid.UUID, BoundRemoteHandle[Any]] = {}

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
        name = type(self).__name__
        return (
            f'{name}(mailbox_id={self.mailbox_id!r}, '
            f'exchange={self.exchange!r})'
        )

    def __str__(self) -> str:
        return f'{type(self).__name__}<{self.mailbox_id}; {self.exchange}>'

    def _message_handler(self, message: Message) -> None:
        if isinstance(message, get_args(RequestMessage)):
            self.request_handler(message)
        elif isinstance(message, get_args(ResponseMessage)):
            try:
                handle = self.bound_handles[message.label]
            except KeyError:
                logger.exception(
                    'Receieved a response message from %s but no handle to '
                    'that agent is bound to %s.',
                    message.src,
                    self,
                )
            else:
                handle._process_response(message)
        else:
            raise AssertionError('Unreachable.')

    def bind(
        self,
        handle: RemoteHandle[BehaviorT_co],
    ) -> BoundRemoteHandle[BehaviorT_co]:
        """Bind a handle to this mailbox.

        Args:
            handle: Remote handle to bind.

        Returns:
            Remote handle bound to this mailbox.
        """
        bound = handle.bind_to_mailbox(self.mailbox_id)
        self.bound_handles[bound.handle_id] = bound
        logger.debug(
            'Bound handle to %s to multiplexer (%s)',
            bound.agent_id,
            self,
        )
        return bound

    def close(self) -> None:
        """Close the multiplexer.

        Closes all handles bound to this mailbox and then closes the mailbox.
        """
        # This will cause listen() to return
        self.close_mailbox()
        self.close_bound_handles()

    def close_bound_handles(self) -> None:
        """Close all handles bound to this mailbox."""
        for key in tuple(self.bound_handles):
            handle = self.bound_handles.pop(key)
            handle.close(wait_futures=False)
        logger.debug('Closed all handles bound to multiplexer (%s)', self)

    def close_mailbox(self) -> None:
        """Close the mailbox."""
        self.exchange.close_mailbox(self.mailbox_id)
        logger.debug('Closed mailbox of multiplexer (%s)', self)

    def listen(self) -> None:
        """Listen for new messages in the mailbox and process them.

        Request messages are processed via the `request_handler`, and response
        messages are dispatched to the handle that created the corresponding
        request.

        Warning:
            This method loops forever, until the mailbox is closed. Thus this
            method is typically run inside of a thread.

        Note:
            Response messages intended for a handle that does not exist
            will be logged and discarded.
        """
        logger.debug('Listening for messages in %s', self)
        mailbox = self.exchange.get_mailbox(self.mailbox_id)

        try:
            while True:
                try:
                    message = mailbox.recv()
                except MailboxClosedError:
                    break
                else:
                    self._message_handler(message)
        finally:
            mailbox.close()
            logger.debug('Finished listening for messages in %s', self)
