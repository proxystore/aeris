from __future__ import annotations

from unittest import mock

import pytest

from academy.exception import HandleClosedError
from academy.exception import MailboxClosedError
from academy.exchange.thread import ThreadExchange
from academy.handle import BoundRemoteHandle
from academy.handle import UnboundRemoteHandle
from academy.identifier import ClientId
from academy.message import PingRequest
from academy.message import PingResponse
from academy.multiplex import MailboxMultiplexer
from testing.behavior import EmptyBehavior


def test_protocol(exchange: ThreadExchange) -> None:
    uid = exchange.register_client()
    with MailboxMultiplexer(
        uid,
        exchange,
        request_handler=lambda _: None,
    ) as mutliplexer:
        assert isinstance(repr(mutliplexer), str)
        assert isinstance(str(mutliplexer), str)


def test_listen_exit_on_mailbox_close(exchange: ThreadExchange) -> None:
    uid = exchange.register_client()
    exchange.terminate(uid)
    with MailboxMultiplexer(
        uid,
        exchange,
        request_handler=lambda _: None,
    ) as multiplexer:
        # Should immediately return because the mailbox is closed.
        multiplexer.listen()


def test_bind_handle(exchange: ThreadExchange) -> None:
    uid = exchange.register_client()
    aid = exchange.register_agent(EmptyBehavior)
    unbound = UnboundRemoteHandle(exchange, aid)
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
    uid = exchange.register_client()
    aid = exchange.register_agent(EmptyBehavior)
    unbound = UnboundRemoteHandle(exchange, aid)
    with MailboxMultiplexer(
        uid,
        exchange,
        request_handler=lambda _: None,
    ) as multiplexer:
        multiplexer.bind(unbound)
        multiplexer.bind(unbound)


def test_request_message_handler(exchange: ThreadExchange) -> None:
    uid = exchange.register_client()
    handler = mock.MagicMock()
    request = PingRequest(src=ClientId.new(), dest=uid)

    with MailboxMultiplexer(
        uid,
        exchange,
        request_handler=handler,
    ) as multiplexer:
        with mock.patch(
            'academy.exchange.thread.ThreadMailbox.recv',
            side_effect=(request, MailboxClosedError(uid)),
        ):
            multiplexer.listen()

        handler.assert_called_once()


def test_response_message_handler(exchange: ThreadExchange) -> None:
    uid = exchange.register_client()
    aid = exchange.register_agent(EmptyBehavior)
    unbound = UnboundRemoteHandle(exchange, aid)

    with MailboxMultiplexer(
        uid,
        exchange,
        request_handler=lambda _: None,
    ) as multiplexer:
        bound = multiplexer.bind(unbound)
        response = PingResponse(src=aid, dest=uid, label=bound.handle_id)
        with mock.patch.object(bound, '_process_response') as mocked:
            with mock.patch(
                'academy.exchange.thread.ThreadMailbox.recv',
                side_effect=(response, MailboxClosedError(uid)),
            ):
                multiplexer.listen()
            mocked.assert_called_once()


def test_response_message_handler_bad_src(exchange: ThreadExchange) -> None:
    uid = exchange.register_client()
    aid = exchange.register_agent(EmptyBehavior)
    unbound = UnboundRemoteHandle(exchange, aid)
    response = PingResponse(src=ClientId.new(), dest=uid)

    with MailboxMultiplexer(
        uid,
        exchange,
        request_handler=lambda _: None,
    ) as multiplexer:
        with mock.patch(
            'academy.exchange.thread.ThreadMailbox.recv',
            side_effect=(response, MailboxClosedError(uid)),
        ):
            bound = multiplexer.bind(unbound)
            with mock.patch.object(bound, '_process_response') as mocked:
                multiplexer.listen()
                # Message handler will not have a valid handle to pass
                # the message to so it just logs an exception and moves on.
                mocked.assert_not_called()
