from __future__ import annotations

import base64
import pickle
import uuid
from typing import Any
from typing import Literal
from typing import Optional
from typing import Union

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import field_serializer
from pydantic import field_validator
from pydantic import TypeAdapter

from aeris.identifier import Identifier

NO_RESULT = object()


class BaseMessage(BaseModel):
    """Base exchange message type."""

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra='forbid',
        frozen=True,
        use_enum_values=True,
        validate_default=True,
    )

    mid: uuid.UUID = Field(default_factory=uuid.uuid4)
    src: Identifier
    dest: Identifier

    def __str__(self) -> str:
        name = type(self).__name__
        return f'{name}<from {self.src}; to {self.dest}; {self.mid}>'

    @classmethod
    def model_from_json(cls, data: str) -> Message:
        """Reconstruct a specific message from a JSON dump.

        Example:
            ```python
            from aeris.message import BaseMessage, ActionRequest

            message = ActionRequest(...)
            dump = message.model_dump_json()
            assert BaseMessage.model_from_json(dump) == message
            ```
        """
        return TypeAdapter(Message).validate_json(data)


class ActionRequest(BaseMessage):
    """Action request message.

    When this message is dumped to a JSON string, the `args` and `kwargs`
    are pickled and then base64 encoded to a string. This can have non-trivial
    time and space overheads for large arguments.
    """

    action: str
    args: tuple[Any, ...] = Field(default_factory=tuple)
    kwargs: dict[str, Any] = Field(default_factory=dict)
    kind: Literal['action-request'] = Field('action-request', repr=False)

    @field_serializer('args', 'kwargs', when_used='json')
    def _pickle_and_encode_obj(self, obj: Any) -> str:
        raw = pickle.dumps(obj)
        return base64.b64encode(raw).decode('utf-8')

    @field_validator('args', 'kwargs', mode='before')
    @classmethod
    def _decode_pickled_obj(cls, obj: Any) -> Any:
        if not isinstance(obj, str):
            return obj
        return pickle.loads(base64.b64decode(obj))

    def response(
        self,
        *,
        result: Any = NO_RESULT,
        exception: Exception | None = None,
    ) -> ActionResponse:
        """Construct response to action request.

        Args:
            result: Result of the action.
            exception: Exception raised by the action.

        Raises:
            TypeError: if neither or both of `exception` and `result` are set.
        """
        if exception is None and result is NO_RESULT:
            raise TypeError('One of exception or result must be provided.')
        elif exception is not None and result is not NO_RESULT:
            raise TypeError('Cannot provide exception and result.')

        return ActionResponse(
            mid=self.mid,
            src=self.dest,
            dest=self.src,
            action=self.action,
            result=None if result is NO_RESULT else result,
            exception=exception,
        )


class ActionResponse(BaseMessage):
    """Action response message."""

    action: str
    result: Any = None
    exception: Optional[Exception] = None  # noqa: UP007
    kind: Literal['action-response'] = Field('action-response', repr=False)

    @field_serializer('exception', 'result', when_used='json')
    def _pickle_and_encode_obj(self, obj: Any) -> str | None:
        if obj is None:
            return None
        raw = pickle.dumps(obj)
        return base64.b64encode(raw).decode('utf-8')

    @field_validator('exception', 'result', mode='before')
    @classmethod
    def _decode_pickled_obj(cls, obj: Any) -> Any:
        if not isinstance(obj, str):
            return obj
        return pickle.loads(base64.b64decode(obj))

    def __eq__(self, other: object, /) -> bool:
        if not isinstance(other, ActionResponse):
            return False
        return (
            self.mid == other.mid
            and self.src == other.src
            and self.dest == other.dest
            and self.action == other.action
            and self.result == other.result
            # Custom __eq__ is required because exception instances need
            # to be compared by type. I.e., Exception() == Exception() is
            # always False.
            and type(self.exception) is type(other.exception)
        )


class PingRequest(BaseMessage):
    """Ping request message."""

    kind: Literal['ping-request'] = Field('ping-request', repr=False)

    def response(self) -> PingResponse:
        """Construct a ping response message."""
        return PingResponse(mid=self.mid, src=self.dest, dest=self.src)


class PingResponse(BaseMessage):
    """Ping response message."""

    kind: Literal['ping-response'] = Field('ping-response', repr=False)


class ShutdownRequest(BaseMessage):
    """Shutdown request message."""

    kind: Literal['shutdown-request'] = Field('shutdown-request', repr=False)


Message = Union[
    ActionRequest,
    ActionResponse,
    PingRequest,
    PingResponse,
    ShutdownRequest,
]
"""Message union type for type annotations.

Tip:
    This is a parameterized generic type meaning that this type cannot
    be used for [`isinstance`][`builtins.isinstance`] checks:
    ```python
    isinstance(message, Message)  # Fails
    ```
    Instead, use [`typing.get_args()`][typing.get_args]:
    ```
    from typing import get_args

    isinstance(message, get_args(Message))  # Works
    ```
"""
