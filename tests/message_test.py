from __future__ import annotations

import pytest

from aeris.identifier import AgentIdentifier
from aeris.identifier import ClientIdentifier
from aeris.message import ActionRequest
from aeris.message import ActionResponse
from aeris.message import PingRequest
from aeris.message import PingResponse


def test_message_repr() -> None:
    request = ActionRequest(
        src=ClientIdentifier.new(),
        dest=AgentIdentifier.new(),
        action='foo',
    )
    assert isinstance(repr(request), str)
    assert isinstance(str(request), str)


def test_construct_action_response() -> None:
    request = ActionRequest(
        src=ClientIdentifier.new(),
        dest=AgentIdentifier.new(),
        action='foo',
    )

    assert isinstance(request.response(result=42), ActionResponse)

    with pytest.raises(
        TypeError,
        match='One of exception or result must be provided',
    ):
        request.response()

    with pytest.raises(TypeError, match='Cannot provide exception and result'):
        request.response(exception=Exception(), result=42)


def test_construct_ping_response() -> None:
    request = PingRequest(
        src=ClientIdentifier.new(),
        dest=AgentIdentifier.new(),
    )

    assert isinstance(request.response(), PingResponse)
