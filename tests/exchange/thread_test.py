from __future__ import annotations

import pickle
from typing import Any

import pytest

from aeris.exception import BadIdentifierError
from aeris.exception import MailboxClosedError
from aeris.exchange import Exchange
from aeris.exchange.thread import ThreadExchange
from aeris.handle import RemoteHandle
from aeris.identifier import AgentIdentifier
from aeris.identifier import ClientIdentifier
from aeris.message import PingRequest


def test_basic_usage() -> None:
    with ThreadExchange() as exchange:
        assert isinstance(exchange, Exchange)
        assert isinstance(repr(exchange), str)
        assert isinstance(str(exchange), str)

        aid = exchange.create_agent()
        cid = exchange.create_client()
        exchange.create_mailbox(cid)  # Idempotency check

        assert isinstance(aid, AgentIdentifier)
        assert isinstance(cid, ClientIdentifier)

        mailbox = exchange.get_mailbox(aid)
        assert mailbox.exchange is exchange

        for _ in range(3):
            message = PingRequest(src=cid, dest=aid)
            exchange.send(aid, message)
            assert mailbox.recv() == message

        mailbox.close()
        exchange.close_mailbox(aid)
        exchange.close_mailbox(cid)
        exchange.close_mailbox(cid)  # Idempotency check


def test_bad_identifier_error() -> None:
    with ThreadExchange() as exchange:
        uid = AgentIdentifier.new()
        with pytest.raises(BadIdentifierError):
            exchange.send(uid, PingRequest(src=uid, dest=uid))
        with pytest.raises(BadIdentifierError):
            exchange.get_mailbox(uid)


def test_mailbox_closed_error() -> None:
    with ThreadExchange() as exchange:
        aid = exchange.create_agent()
        mailbox = exchange.get_mailbox(aid)
        exchange.close_mailbox(aid)
        with pytest.raises(MailboxClosedError):
            exchange.send(aid, PingRequest(src=aid, dest=aid))
        with pytest.raises(MailboxClosedError):
            mailbox.recv()
        mailbox.close()


def test_create_handle_to_client() -> None:
    with ThreadExchange() as exchange:
        aid = exchange.create_agent()
        handle: RemoteHandle[Any] = exchange.create_handle(aid)
        handle.close()

        with pytest.raises(TypeError, match='Handle must be created from an'):
            exchange.create_handle(ClientIdentifier.new())  # type: ignore[arg-type]


def test_non_pickleable() -> None:
    with ThreadExchange() as exchange:
        with pytest.raises(pickle.PicklingError):
            pickle.dumps(exchange)
