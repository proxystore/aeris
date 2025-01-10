from __future__ import annotations

from typing import get_args

import pytest

from aeris.exchange.message import BaseExchangeMessage
from aeris.exchange.message import ExchangeMessage
from aeris.exchange.message import ForwardMessage
from aeris.exchange.message import RegisterMessage
from aeris.exchange.message import ResponseMessage
from aeris.exchange.message import UnregisterMessage
from aeris.identifier import AgentIdentifier


@pytest.mark.parametrize(
    'message',
    (
        RegisterMessage(src=AgentIdentifier.new()),
        UnregisterMessage(src=AgentIdentifier.new()),
        ForwardMessage(
            src=AgentIdentifier.new(),
            dest=AgentIdentifier.new(),
            message='message',
        ),
        ResponseMessage(src=AgentIdentifier.new(), op='forward'),
    ),
)
def test_exchange_message_serialize(message: ExchangeMessage) -> None:
    raw = message.model_serialize()
    recreated = BaseExchangeMessage.model_deserialize(raw)
    assert isinstance(recreated, get_args(ExchangeMessage))
    assert message == recreated


@pytest.mark.parametrize(
    'message',
    (
        RegisterMessage(src=AgentIdentifier.new()),
        UnregisterMessage(src=AgentIdentifier.new()),
        ForwardMessage(
            src=AgentIdentifier.new(),
            dest=AgentIdentifier.new(),
            message='message',
        ),
    ),
)
def test_create_response(
    message: RegisterMessage | UnregisterMessage | ForwardMessage,
) -> None:
    response = message.response(error='error')
    assert isinstance(response, ResponseMessage)
    assert response.op == message.kind
    assert not response.success
