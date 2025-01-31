from __future__ import annotations

import asyncio
import logging
from concurrent.futures import Future
from concurrent.futures import ProcessPoolExecutor

from aeris.behavior import action
from aeris.behavior import Behavior
from aeris.exchange.simple import serve_forever
from aeris.exchange.simple import SimpleServer
from aeris.exchange.simple import spawn_simple_exchange
from aeris.handle import Handle
from aeris.launcher.executor import ExecutorLauncher
from aeris.logging import init_logging

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


def run_exchange_server() -> None:
    server = SimpleServer('localhost', EXCHANGE_PORT)
    asyncio.run(serve_forever(server))


def main() -> int:
    init_logging(logging.INFO)

    with spawn_simple_exchange('localhost', EXCHANGE_PORT) as exchange:
        # Agents are launched using a Launcher. The ExecutorLauncher can use
        # any concurrent.futures.Executor (here, a ProcessPoolExecutor) to
        # execute agents. The launcher is initialized to use the simple
        # exchange that we just set up for agent and client communication.
        with ExecutorLauncher(ProcessPoolExecutor(max_workers=3)) as launcher:
            # Initialize and launch each of the three agents. The
            # bind_as_client method returns a client handle to the agent that
            # can be used to send commands to that agent.
            lowerer = launcher.launch(Lowerer(), exchange).bind_as_client()
            reverser = launcher.launch(Reverser(), exchange).bind_as_client()
            coordinator = launcher.launch(
                Coordinator(lowerer, reverser),
                exchange,
            ).bind_as_client()

            text = 'DEADBEEF'
            expected = 'feebdaed'

            future: Future[str] = coordinator.action('process', text)
            assert future.result() == expected

            # Instruct all agents to shutdown
            lowerer.shutdown()
            reverser.shutdown()
            coordinator.shutdown()

            # Close client handles to all agents
            lowerer.close()
            reverser.close()
            coordinator.close()

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
