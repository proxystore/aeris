from __future__ import annotations

import pickle
from typing import Any

import pytest

from aeris.exception import BadEntityIdError
from aeris.exception import MailboxClosedError
from aeris.exchange import Exchange
from aeris.exchange.thread import ThreadExchange
from aeris.identifier import AgentId
from aeris.identifier import ClientId
from aeris.message import PingRequest
from testing.behavior import EmptyBehavior


def test_basic_usage() -> None:
    with ThreadExchange() as exchange:
        assert isinstance(exchange, Exchange)
        assert isinstance(repr(exchange), str)
        assert isinstance(str(exchange), str)

        aid = exchange.register_agent(EmptyBehavior)
        cid = exchange.register_client()
        exchange.create_mailbox(cid)  # Idempotency check

        assert isinstance(aid, AgentId)
        assert isinstance(cid, ClientId)

        mailbox = exchange.get_mailbox(aid)
        assert mailbox.exchange is exchange

        for _ in range(3):
            message = PingRequest(src=cid, dest=aid)
            exchange.send(aid, message)
            assert mailbox.recv() == message

        mailbox.close()
        exchange.terminate(aid)
        exchange.terminate(cid)
        exchange.terminate(cid)  # Idempotency check


def test_bad_identifier_error() -> None:
    with ThreadExchange() as exchange:
        uid: AgentId[Any] = AgentId.new()
        with pytest.raises(BadEntityIdError):
            exchange.send(uid, PingRequest(src=uid, dest=uid))
        with pytest.raises(BadEntityIdError):
            exchange.get_mailbox(uid)


def test_mailbox_closed_error() -> None:
    with ThreadExchange() as exchange:
        aid = exchange.register_agent(EmptyBehavior)
        mailbox = exchange.get_mailbox(aid)
        exchange.terminate(aid)
        with pytest.raises(MailboxClosedError):
            exchange.send(aid, PingRequest(src=aid, dest=aid))
        with pytest.raises(MailboxClosedError):
            mailbox.recv()
        mailbox.close()


def test_get_handle_to_client() -> None:
    with ThreadExchange() as exchange:
        aid = exchange.register_agent(EmptyBehavior)
        handle = exchange.get_handle(aid)
        handle.close()

        with pytest.raises(TypeError, match='Handle must be created from an'):
            exchange.get_handle(ClientId.new())  # type: ignore[arg-type]


def test_non_pickleable() -> None:
    with ThreadExchange() as exchange:
        with pytest.raises(pickle.PicklingError):
            pickle.dumps(exchange)
