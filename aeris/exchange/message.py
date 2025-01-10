"""Exchange message types.

Warning:
    This module is not considered part of the public API. These message
    types are helpers for exchange implementations.
"""

from __future__ import annotations

from typing import Literal
from typing import Union

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import TypeAdapter

from aeris.identifier import Identifier


class BaseExchangeMessage(BaseModel):
    """Base exchange message."""

    model_config = ConfigDict(
        extra='forbid',
        use_enum_values=True,
        validate_default=True,
    )

    @classmethod
    def model_deserialize(cls, raw: bytes) -> ExchangeMessage:
        """Reconstruct a specific message from a serialized message.

        Example:
            ```python
            from aeris.exchange.message import BaseExchangeMessage
            from aeris.exchange.message import RegisterMessage

            message = RegisterMessage(...)
            raw = RegisterMessage.model_serialize()
            assert BaseExchangeMessage.model_deserialize(raw) == message
            ```
        """
        dump = raw.decode()
        return TypeAdapter(ExchangeMessage).validate_json(dump)

    def model_serialize(self) -> bytes:
        """Serialize a message to a bytestring.

        Messages are serialized by dumping the model to a JSON-compatible
        string and then encoding the string into bytes.
        """
        dump = self.model_dump_json()
        return dump.encode()


class RegisterMessage(BaseExchangeMessage):
    """Exchange registration request message.

    Args:
        src: Identifier of the entity registering with the exchange.
    """

    src: Identifier
    kind: Literal['register'] = Field('register', repr=False)

    def response(self, error: str | None = None) -> ResponseMessage:
        """Construct an exchange response message.

        Args:
            error: Error message if the exchange could not complete the
                request.
        """
        return ResponseMessage(src=self.src, op='register', error=error)


class UnregisterMessage(BaseExchangeMessage):
    """Exchange unregistration request message.

    Args:
        src: Identifier of the entity unregistering with the exchange.
    """

    src: Identifier
    kind: Literal['unregister'] = Field('unregister', repr=False)

    def response(self, error: str | None = None) -> ResponseMessage:
        """Construct an exchange response message.

        Args:
            error: Error message if the exchange could not complete the
                request.
        """
        return ResponseMessage(src=self.src, op='unregister', error=error)


class ForwardMessage(BaseExchangeMessage):
    """Message for exchange to forward to another mailbox.

    Args:
        src: Source of the message.
        dest: Destination of the message.
        message: The message to send. This is typically a JSON string
            of a [`Message`][aeris.message.Message].
    """

    src: Identifier
    dest: Identifier
    message: str
    kind: Literal['forward'] = Field('forward', repr=False)

    def response(self, error: str | None = None) -> ResponseMessage:
        """Construct an exchange response message.

        Args:
            error: Error message if the exchange could not complete the
                request.
        """
        return ResponseMessage(src=self.src, op='forward', error=error)


class ResponseMessage(BaseExchangeMessage):
    """Response message from the exchange to clients.

    Args:
        src: Identifier of the client that made the request.
        op: Type of the request the exchange received.
        error: Error message from the exchange.
    """

    src: Identifier
    op: Literal['forward', 'register', 'unregister']
    error: str | None = None
    kind: Literal['response'] = Field('response', repr=False)

    @property
    def success(self) -> bool:
        """Check if the exchange completed the request.

        If `False`, `error` will be a string containing an error message
        from the exchange.
        """
        return self.error is None


ExchangeMessage = Union[
    RegisterMessage,
    UnregisterMessage,
    ForwardMessage,
    ResponseMessage,
]
"""Exchange message union type for type annotations.

Tip:
    This is a parameterized generic type meaning that this type cannot
    be used for [`isinstance`][`builtins.isinstance`] checks:
    ```python
    isinstance(message, ExchangeMessage)  # Fails
    ```
    Instead, use [`typing.get_args()`][typing.get_args]:
    ```
    from typing import get_args

    isinstance(message, get_args(ExchangeMessage))  # Works
    ```
"""
