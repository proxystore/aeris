from __future__ import annotations

import threading

from aeris.agent import agent
from aeris.agent import AgentRunner
from aeris.agent import loop


@agent
class WaitingAgent:
    def __init__(self) -> None:
        self.setup_event = threading.Event()
        self.loop_event = threading.Event()
        self.shutdown_event = threading.Event()

    def setup(self) -> None:
        self.setup_event.set()

    def shutdown(self) -> None:
        self.shutdown_event.set()

    @loop
    def waiter(self, shutdown: threading.Event) -> None:
        self.loop_event.wait()
        shutdown.wait()

    @loop
    def setter(self, shutdown: threading.Event) -> None:
        self.loop_event.set()
        shutdown.wait()


def test_agent_runner_basic() -> None:
    instance = WaitingAgent()
    runner = AgentRunner(instance)

    def run() -> None:
        runner()

    thread = threading.Thread(target=run)
    thread.start()
    runner.agent.setup_event.wait()
    runner.agent.loop_event.wait()
    runner.shutdown()
    thread.join(timeout=1)

    assert instance.setup_event.is_set()
    assert instance.shutdown_event.is_set()


def test_agent_runner_shutdown() -> None:
    instance = WaitingAgent()
    runner = AgentRunner(instance)

    runner.shutdown()
    runner.run()

    assert instance.setup_event.is_set()
    assert instance.shutdown_event.is_set()
