from __future__ import annotations

import pickle
from typing import Any

import pytest

from academy.behavior import Behavior
from academy.exception import BadEntityIdError
from academy.exception import MailboxClosedError
from academy.exchange import Exchange
from academy.exchange.thread import ThreadExchange
from academy.identifier import AgentId
from academy.identifier import ClientId
from academy.message import PingRequest
from testing.behavior import EmptyBehavior


def test_basic_usage() -> None:
    with ThreadExchange() as exchange:
        assert isinstance(exchange, Exchange)
        assert isinstance(repr(exchange), str)
        assert isinstance(str(exchange), str)

        aid = exchange.register_agent(EmptyBehavior)
        exchange.register_agent(EmptyBehavior, agent_id=aid)
        cid = exchange.register_client()

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


def test_discover() -> None:
    class A(Behavior): ...

    class B(Behavior): ...

    class C(B): ...

    with ThreadExchange() as exchange:
        bid = exchange.register_agent(B)
        cid = exchange.register_agent(C)
        did = exchange.register_agent(C)
        exchange.terminate(did)

        assert len(exchange.discover(A)) == 0
        assert exchange.discover(B, allow_subclasses=False) == (bid,)
        assert exchange.discover(B, allow_subclasses=True) == (bid, cid)
