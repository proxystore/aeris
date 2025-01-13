from __future__ import annotations

from typing import get_args

import pytest

from aeris.exchange.message import BaseExchangeMessage
from aeris.exchange.message import ExchangeMessage
from aeris.exchange.message import ExchangeRequestMessage
from aeris.exchange.message import ExchangeResponseMessage
from aeris.exchange.message import ForwardMessage
from aeris.exchange.message import RegisterMessage
from aeris.exchange.message import UnregisterMessage
from aeris.identifier import AgentIdentifier
from aeris.message import PingRequest


@pytest.mark.parametrize(
    'message',
    (
        RegisterMessage(src=AgentIdentifier.new()),
        UnregisterMessage(src=AgentIdentifier.new()),
        ForwardMessage(
            src=AgentIdentifier.new(),
            dest=AgentIdentifier.new(),
            message=PingRequest(
                src=AgentIdentifier.new(),
                dest=AgentIdentifier.new(),
            ),
        ),
        ExchangeResponseMessage(
            src=AgentIdentifier.new(),
            request=RegisterMessage(src=AgentIdentifier.new()),
        ),
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
            message=PingRequest(
                src=AgentIdentifier.new(),
                dest=AgentIdentifier.new(),
            ),
        ),
    ),
)
def test_create_response(message: ExchangeRequestMessage) -> None:
    response = message.response(error='error')
    assert isinstance(response, ExchangeResponseMessage)
    assert isinstance(response.request, type(message))
    assert not response.success
