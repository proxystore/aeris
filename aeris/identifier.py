from __future__ import annotations

import sys
import uuid
from typing import Literal
from typing import Optional
from typing import Union

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field


class BaseIdentifier(BaseModel):
    """Unique identifier of an entity in a multi-agent system.

    Internally an entity is represented by a UUID. An identify can also
    have an optional name which is only used for human-readability in logging.

    Args:
        uid: Unique identifier for the entity.
        name: Optional human-readable name for the entity.
    """

    model_config = ConfigDict(
        extra='forbid',
        frozen=True,
        validate_default=True,
    )

    uid: uuid.UUID = Field()
    name: Optional[str] = Field(None)  # noqa: UP007

    @classmethod
    def new(cls, name: str | None = None) -> Self:
        """Create a new entity identifier.

        Args:
            name: Optional human-readable name for the entity.
        """
        return cls(uid=uuid.uuid4(), name=name)

    def __eq__(self, other: object, /) -> bool:
        return isinstance(other, type(self)) and self.uid == other.uid

    def __hash__(self) -> int:
        return hash(type(self).__name__) + hash(self.uid)


class AgentIdentifier(BaseIdentifier):
    """Unique identifier of an agent in a multi-agent system."""

    role: Literal['agent'] = Field('agent', repr=False)

    def __str__(self) -> str:
        name = self.name if self.name is not None else str(self.uid)[:8]
        return f'AgentID<{name}>'


class ClientIdentifier(BaseIdentifier):
    """Unique identifier of a client in a multi-agent system."""

    role: Literal['client'] = Field('client', repr=False)

    def __str__(self) -> str:
        name = self.name if self.name is not None else str(self.uid)[:8]
        return f'ClientID<{name}>'


Identifier = Union[AgentIdentifier, ClientIdentifier]
"""Identifier union type for type annotations."""
