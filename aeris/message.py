from __future__ import annotations

import enum
import uuid
from typing import Any
from typing import Optional

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field

from aeris.identifier import Identifier

NO_RESULT = object()


class MessageKind(enum.Enum):
    """Message variants."""

    ACTION = 'action'
    PING = 'ping'
    SHUTDOWN = 'shutdown'


class Message(BaseModel):
    """Base exchange message type."""

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra='forbid',
        frozen=True,
        use_enum_values=True,
        validate_default=True,
    )

    kind: MessageKind
    mid: uuid.UUID = Field(default_factory=uuid.uuid4)
    src: Identifier
    dest: Identifier


class ActionRequest(Message):
    """Action request message."""

    kind: MessageKind = Field(MessageKind.ACTION)
    action: str
    args: tuple[Any, ...] = Field(default_factory=tuple)
    kwargs: dict[str, Any] = Field(default_factory=dict)

    def response(
        self,
        *,
        exception: Exception | None = None,
        result: Any = NO_RESULT,
    ) -> ActionResponse:
        """Construct response to action request.

        Args:
            exception: Exception raised by the action.
            result: Result of the action.

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
            exception=exception,
            result=None if result is NO_RESULT else result,
        )


class ActionResponse(Message):
    """Action response message."""

    kind: MessageKind = Field(MessageKind.ACTION)
    action: str
    exception: Optional[Exception] = None  # noqa: UP007
    result: Any = None


class PingRequest(Message):
    """Ping request message."""

    kind: MessageKind = Field(MessageKind.PING)

    def response(self) -> PingResponse:
        """Construct a ping response message."""
        return PingResponse(mid=self.mid, src=self.dest, dest=self.src)


class PingResponse(Message):
    """Ping response message."""

    kind: MessageKind = Field(MessageKind.PING)


class ShutdownRequest(Message):
    """Shutdown request message."""

    kind: MessageKind = Field(MessageKind.SHUTDOWN)
