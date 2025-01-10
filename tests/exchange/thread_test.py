from __future__ import annotations

import pickle

import pytest

from aeris.exception import BadIdentifierError
from aeris.exchange import Exchange
from aeris.exchange import Mailbox
from aeris.exchange import MailboxClosedError
from aeris.exchange.thread import ThreadExchange
from aeris.identifier import AgentIdentifier
from aeris.identifier import ClientIdentifier
from aeris.message import PingRequest


def test_protocol() -> None:
    with ThreadExchange() as exchange:
        assert isinstance(exchange, Exchange)
        assert isinstance(str(exchange), str)

        agent_id = exchange.register_agent()
        client_id = exchange.register_client()

        assert isinstance(agent_id, AgentIdentifier)
        assert isinstance(client_id, ClientIdentifier)

        mailbox = exchange.get_mailbox(agent_id)
        assert isinstance(mailbox, Mailbox)
        with mailbox:
            pass

        mailbox = exchange.get_mailbox(client_id)
        assert isinstance(mailbox, Mailbox)
        with mailbox:
            pass

        handle = exchange.create_handle(agent_id)
        handle.close()


def test_mailbox_send_recv() -> None:
    with ThreadExchange() as exchange:
        agent_id = exchange.register_agent()
        mailbox = exchange.get_mailbox(agent_id)
        assert mailbox is not None

        message = PingRequest(
            src=ClientIdentifier.new(),
            dest=ClientIdentifier.new(),
        )
        mailbox.send(message)
        assert mailbox.recv() == message

        mailbox.close()
        mailbox.close()

        with pytest.raises(MailboxClosedError):
            mailbox.send(message)
        with pytest.raises(MailboxClosedError):
            mailbox.recv()


def test_get_mailbox_unknown_id() -> None:
    with ThreadExchange() as exchange:
        cid = ClientIdentifier.new()
        with pytest.raises(BadIdentifierError):
            exchange.get_mailbox(cid)


def test_create_handle_to_client() -> None:
    with ThreadExchange() as exchange:
        with pytest.raises(TypeError, match='Handle must be created from an'):
            exchange.create_handle(ClientIdentifier.new())  # type: ignore[arg-type]


def test_unregister_entity() -> None:
    with ThreadExchange() as exchange:
        agent_id = exchange.register_agent()
        exchange.unregister(agent_id)

        agent_id = exchange.register_agent()
        mailbox = exchange.get_mailbox(agent_id)
        assert mailbox is not None

        exchange.unregister(agent_id)
        with pytest.raises(BadIdentifierError):
            exchange.get_mailbox(agent_id)

        with pytest.raises(MailboxClosedError):
            mailbox.recv()


def test_non_pickleable() -> None:
    with ThreadExchange() as exchange:
        with pytest.raises(pickle.PicklingError):
            pickle.dumps(exchange)
