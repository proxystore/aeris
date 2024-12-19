from __future__ import annotations

import pytest

from aeris.exchange import Exchange
from aeris.exchange import Mailbox
from aeris.exchange.thread import ThreadExchange
from aeris.identifier import ClientIdentifier
from aeris.identifier import Role


def test_protocol() -> None:
    exchange = ThreadExchange()
    assert isinstance(exchange, Exchange)

    agent_id = exchange.register_agent()
    client_id = exchange.register_client()

    assert agent_id.role == Role.AGENT
    assert client_id.role == Role.CLIENT

    assert isinstance(exchange.get_mailbox(agent_id), Mailbox)
    assert isinstance(exchange.get_mailbox(client_id), Mailbox)

    assert exchange.create_handle(agent_id) is not None


def test_mailbox_send_recv() -> None:
    exchange = ThreadExchange()
    agent_id = exchange.register_agent()
    mailbox = exchange.get_mailbox(agent_id)
    assert mailbox is not None

    mailbox.send('message')
    assert mailbox.recv() == 'message'


def test_create_handle_to_client() -> None:
    exchange = ThreadExchange()
    with pytest.raises(TypeError, match='Handle must be created from an'):
        exchange.create_handle(ClientIdentifier.new())  # type: ignore[arg-type]
