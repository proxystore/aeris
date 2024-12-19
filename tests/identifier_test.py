from __future__ import annotations

import uuid

from aeris.identifier import AgentIdentifier
from aeris.identifier import ClientIdentifier
from aeris.identifier import Role


def tests_identifier_equality() -> None:
    aid = AgentIdentifier.new()
    cid = ClientIdentifier.new()

    assert aid.role == Role.AGENT
    assert cid.role == Role.CLIENT

    assert aid != aid._uid
    assert aid != cid
    assert hash(aid) != hash(cid)

    uuid_ = uuid.uuid4()
    aid1 = AgentIdentifier(uuid_)
    aid2 = AgentIdentifier(uuid_)
    cid1 = ClientIdentifier(uuid_)

    assert aid1 == aid2
    assert hash(aid1) == hash(aid2)
    assert aid1 != cid1
    assert hash(aid1) != hash(cid1)
