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
    AGENT = 'agent'
    CLIENT = 'client'


class Identifier(abc.ABC):
    def __init__(self, uid: uuid.UUID) -> None:
        self._uid = uid

    @classmethod
    def new(cls) -> Self:
        return cls(uuid.uuid4())

    @property
    @abc.abstractmethod
    def role(self) -> Role: ...

    def __eq__(self, other: object, /) -> bool:
        if isinstance(other, Identifier):
            return self._uid == other._uid and self.role == other.role
        else:
            return False

    def __hash__(self) -> int:
        return hash(self.role) + hash(self._uid)


class AgentIdentifier(Identifier):
    @property
    def role(self) -> Role:
        return Role.AGENT


class ClientIdentifier(Identifier):
    @property
    def role(self) -> Role:
        return Role.CLIENT
