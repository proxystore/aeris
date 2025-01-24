from __future__ import annotations

from collections.abc import Generator

import pytest

from aeris.exchange import Exchange
from aeris.exchange.thread import ThreadExchange
from aeris.launcher.thread import ThreadLauncher


@pytest.fixture
def exchange() -> Generator[Exchange]:
    with ThreadExchange() as exchange:
        yield exchange


@pytest.fixture
def launcher(exchange: Exchange) -> Generator[ThreadLauncher]:
    with ThreadLauncher(exchange) as launcher:
        yield launcher
