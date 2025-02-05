from __future__ import annotations

import logging
from concurrent.futures import Future
from concurrent.futures import ProcessPoolExecutor

from aeris.behavior import action
from aeris.behavior import Behavior
from aeris.exchange.simple import spawn_simple_exchange
from aeris.handle import Handle
from aeris.launcher.executor import ExecutorLauncher
from aeris.logging import init_logging
from aeris.manager import Manager

EXCHANGE_PORT = 5346
logger = logging.getLogger(__name__)


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
    init_logging(logging.INFO)

    with spawn_simple_exchange('localhost', EXCHANGE_PORT) as exchange:
        with Manager(
            exchange=exchange,
            # Agents are launched using a Launcher. The ExecutorLauncher can
            # use any concurrent.futures.Executor (here, a ProcessPoolExecutor)
            # to execute agents.
            launcher=ExecutorLauncher(ProcessPoolExecutor(max_workers=3)),
        ) as manager:
            # Initialize and launch each of the three agents. The returned
            # type is a handle to that agent used to invoke actions.
            lowerer = manager.launch(Lowerer())
            reverser = manager.launch(Reverser())
            coordinator = manager.launch(Coordinator(lowerer, reverser))

            text = 'DEADBEEF'
            expected = 'feebdaed'

            future: Future[str] = coordinator.action('process', text)
            assert future.result() == expected

        # Upon exit, the Manager context will instruct each agent to shutdown
        # and then close the handles, exchange, and launcher interfaces.

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
