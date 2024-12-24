from __future__ import annotations

import abc
import enum
import sys
import uuid

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self


class Role(enum.Enum):
    """Roles of entities in a multi-agent system."""

    AGENT = 'agent'
    CLIENT = 'client'


class Identifier(abc.ABC):
    """Unique identifier of an entity in a multi-agent system.

    Internally an entity is represented by a UUID. An identify can also
    have an optional name which is only used for human-readability in logging.

    Args:
        uuid: Unique identifier for the entity.
        name: Optional human-readable name for the entity.
    """

    def __init__(self, uid: uuid.UUID, *, name: str | None = None) -> None:
        self._uid = uid
        self._name = name

    @classmethod
    def new(cls, name: str | None = None) -> Self:
        """Create a new entity identifier.

        Args:
            name: Optional human-readable name for the entity.
        """
        return cls(uuid.uuid4(), name=name)

    @property
    def name(self) -> str | None:
        """Human-readable name of entity."""
        return self._name

    @property
    @abc.abstractmethod
    def role(self) -> Role:
        """Get the role of this entity."""
        ...

    def __eq__(self, other: object, /) -> bool:
        if isinstance(other, Identifier):
            return self._uid == other._uid and self.role == other.role
        else:
            return False

    def __hash__(self) -> int:
        return hash(self.role) + hash(self._uid)

    def __repr__(self) -> str:
        if self.name is not None:
            return (
                f'{type(self).__name__}(uid={self._uid!s}, name={self.name})'
            )
        else:
            return f'{type(self).__name__}(uid={self._uid!s})'

    def __str__(self) -> str:
        kind = f'{self.role.name.title()}ID'
        if self.name is not None:
            uid_bits = str(self._uid)[:8]
            return f'{kind}<{uid_bits}.., name={self.name}>'
        else:
            return f'{kind}<{self._uid}>'


class AgentIdentifier(Identifier):
    """Unique identifier of an agent in a multi-agent system."""

    @property
    def role(self) -> Role:
        """Get the role of this entity."""
        return Role.AGENT


class ClientIdentifier(Identifier):
    """Unique identifier of a client in a multi-agent system."""

    @property
    def role(self) -> Role:
        """Get the role of this entity."""
        return Role.CLIENT
