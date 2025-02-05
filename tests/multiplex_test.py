from __future__ import annotations

from typing import Any
from unittest import mock

import pytest

from aeris.exception import HandleClosedError
from aeris.exception import MailboxClosedError
from aeris.exchange.thread import ThreadExchange
from aeris.handle import BoundRemoteHandle
from aeris.handle import UnboundRemoteHandle
from aeris.identifier import ClientIdentifier
from aeris.message import PingRequest
from aeris.message import PingResponse
from aeris.multiplex import MailboxMultiplexer


def test_protocol(exchange: ThreadExchange) -> None:
    uid = exchange.create_client()
    with MailboxMultiplexer(
        uid,
        exchange,
        request_handler=lambda _: None,
    ) as mutliplexer:
        assert isinstance(repr(mutliplexer), str)
        assert isinstance(str(mutliplexer), str)


def test_listen_exit_on_mailbox_close(exchange: ThreadExchange) -> None:
    uid = exchange.create_client()
    exchange.close_mailbox(uid)
    with MailboxMultiplexer(
        uid,
        exchange,
        request_handler=lambda _: None,
    ) as multiplexer:
        # Should immediately return because the mailbox is closed.
        multiplexer.listen()


def test_bind_handle(exchange: ThreadExchange) -> None:
    uid = exchange.create_client()
    aid = exchange.create_agent()
    unbound: UnboundRemoteHandle[Any] = UnboundRemoteHandle(exchange, aid)
    with MailboxMultiplexer(
        uid,
        exchange,
        request_handler=lambda _: None,
    ) as multiplexer:
        bound = multiplexer.bind(unbound)
        assert isinstance(bound, BoundRemoteHandle)
        multiplexer.close_bound_handles()
        with pytest.raises(HandleClosedError):
            bound.ping()


def test_bind_duplicate_handle(exchange: ThreadExchange) -> None:
    uid = exchange.create_client()
    aid = exchange.create_agent()
    unbound: UnboundRemoteHandle[Any] = UnboundRemoteHandle(exchange, aid)
    with MailboxMultiplexer(
        uid,
        exchange,
        request_handler=lambda _: None,
    ) as multiplexer:
        multiplexer.bind(unbound)
        multiplexer.bind(unbound)


def test_request_message_handler(exchange: ThreadExchange) -> None:
    uid = exchange.create_client()
    handler = mock.MagicMock()
    request = PingRequest(src=ClientIdentifier.new(), dest=uid)

    with mock.patch.object(
        exchange,
        'recv',
        side_effect=(request, MailboxClosedError(uid)),
    ):
        with MailboxMultiplexer(
            uid,
            exchange,
            request_handler=handler,
        ) as multiplexer:
            multiplexer.listen()

        handler.assert_called_once()


def test_response_message_handler(exchange: ThreadExchange) -> None:
    uid = exchange.create_client()
    aid = exchange.create_agent()
    unbound: UnboundRemoteHandle[Any] = UnboundRemoteHandle(exchange, aid)

    with MailboxMultiplexer(
        uid,
        exchange,
        request_handler=lambda _: None,
    ) as multiplexer:
        bound = multiplexer.bind(unbound)
        response = PingResponse(src=aid, dest=uid, label=bound.handle_id)
        with mock.patch.object(bound, '_process_response') as mocked:
            with mock.patch.object(
                exchange,
                'recv',
                side_effect=(response, MailboxClosedError(uid)),
            ):
                multiplexer.listen()
            mocked.assert_called_once()


def test_response_message_handler_bad_src(exchange: ThreadExchange) -> None:
    uid = exchange.create_client()
    aid = exchange.create_agent()
    unbound: UnboundRemoteHandle[Any] = UnboundRemoteHandle(exchange, aid)
    response = PingResponse(src=ClientIdentifier.new(), dest=uid)

    with mock.patch.object(
        exchange,
        'recv',
        side_effect=(response, MailboxClosedError(uid)),
    ):
        with MailboxMultiplexer(
            uid,
            exchange,
            request_handler=lambda _: None,
        ) as multiplexer:
            bound = multiplexer.bind(unbound)
            with mock.patch.object(bound, '_process_response') as mocked:
                multiplexer.listen()
                # Message handler will not have a valid handle to pass
                # the message to so it just logs an exception and moves on.
                mocked.assert_not_called()
