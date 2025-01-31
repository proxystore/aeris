from __future__ import annotations

import logging
from concurrent.futures import Future

from aeris.behavior import action
from aeris.behavior import Behavior
from aeris.exchange.thread import ThreadExchange
from aeris.handle import Handle
from aeris.launcher.thread import ThreadLauncher
from aeris.logging import init_logging


class Coordinator(Behavior):
    def __init__(
        self,
        lowerer: Handle[Lowerer],
        reverser: Handle[Reverser],
    ) -> None:
        self.lowerer = lowerer
        self.reverser = reverser

    @action
    def process(self, text: str) -> str:
        text = self.lowerer.action('lower', text).result()
        text = self.reverser.action('reverse', text).result()
        return text


class Lowerer(Behavior):
    @action
    def lower(self, text: str) -> str:
        return text.lower()


class Reverser(Behavior):
    @action
    def reverse(self, text: str) -> str:
        return text[::-1]


def main() -> int:
    init_logging(logging.DEBUG)

    with ThreadExchange() as exchange, ThreadLauncher() as launcher:
        lowerer = launcher.launch(Lowerer(), exchange)
        reverser = launcher.launch(Reverser(), exchange)
        coordinator = launcher.launch(
            Coordinator(lowerer, reverser),
            exchange,
        ).bind_as_client()

        text = 'DEADBEEF'
        expected = 'feebdaed'

        future: Future[str] = coordinator.action('process', text)
        assert future.result() == expected

        coordinator.close()
        reverser.close()
        lowerer.close()

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
