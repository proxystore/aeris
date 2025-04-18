from __future__ import annotations

import uuid
from typing import Any

from aeris.identifier import AgentIdentifier
from aeris.identifier import ClientIdentifier


def tests_identifier_equality() -> None:
    aid: AgentIdentifier[Any] = AgentIdentifier.new()
    cid = ClientIdentifier.new()

    assert isinstance(aid, AgentIdentifier)
    assert isinstance(cid, ClientIdentifier)

    assert aid != cid
    assert hash(aid) != hash(cid)

    uid = uuid.uuid4()
    aid1: AgentIdentifier[Any] = AgentIdentifier(uid=uid)
    aid2: AgentIdentifier[Any] = AgentIdentifier(uid=uid)
    cid1 = ClientIdentifier(uid=uid)

    assert aid1 == aid2
    assert str(aid1) == str(aid2)
    assert aid1 != cid1


def tests_identifier_equality_ignore_name() -> None:
    uid = uuid.uuid4()
    aid1: AgentIdentifier[Any] = AgentIdentifier(uid=uid, name='aid1')
    aid2: AgentIdentifier[Any] = AgentIdentifier(uid=uid, name='aid2')

    assert aid1 == aid2
    assert str(aid1) != str(aid2)
