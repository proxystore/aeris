from __future__ import annotations

import threading

from aeris.agent import Agent
from aeris.behavior import loop


class Waiter:
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


def test_agent_run() -> None:
    behavior = Waiter()
    agent = Agent(behavior)

    def run() -> None:
        agent()

    thread = threading.Thread(target=run)
    thread.start()
    agent.behavior.setup_event.wait()
    agent.behavior.loop_event.wait()
    agent.shutdown()
    thread.join(timeout=1)

    assert agent.behavior.setup_event.is_set()
    assert agent.behavior.shutdown_event.is_set()


def test_agent_shutdown() -> None:
    behavior = Waiter()
    agent = Agent(behavior)

    agent.shutdown()
    agent.run()

    assert agent.behavior.setup_event.is_set()
    assert agent.behavior.shutdown_event.is_set()
