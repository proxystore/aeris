from __future__ import annotations

import logging
import multiprocessing
from concurrent.futures import Future
from concurrent.futures import ProcessPoolExecutor

from academy.behavior import action
from academy.behavior import Behavior
from academy.exchange.http import spawn_http_exchange
from academy.handle import Handle
from academy.launcher.executor import ExecutorLauncher
from academy.logging import init_logging
from academy.manager import Manager

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

    with spawn_http_exchange('localhost', EXCHANGE_PORT) as exchange:
        mp_context = multiprocessing.get_context('spawn')
        executor = ProcessPoolExecutor(max_workers=3, mp_context=mp_context)
        with Manager(
            exchange=exchange,
            # Agents are launched using a Launcher. The ExecutorLauncher can
            # use any concurrent.futures.Executor (here, a ProcessPoolExecutor)
            # to execute agents.
            launcher=ExecutorLauncher(executor),
        ) as manager:
            # Initialize and launch each of the three agents. The returned
            # type is a handle to that agent used to invoke actions.
            lowerer = manager.launch(Lowerer())
            reverser = manager.launch(Reverser())
            coordinator = manager.launch(Coordinator(lowerer, reverser))

            text = 'DEADBEEF'
            expected = 'feebdaed'

            future: Future[str] = coordinator.action('process', text)
            logger.info(
                'Invoking process("%s") on %s',
                text,
                coordinator.agent_id,
            )
            assert future.result() == expected
            logger.info('Received result: "%s"', future.result())

        # Upon exit, the Manager context will instruct each agent to shutdown
        # and then close the handles, exchange, and launcher interfaces.

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
