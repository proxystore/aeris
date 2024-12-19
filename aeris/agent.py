from __future__ import annotations

import threading
from concurrent.futures import as_completed
from concurrent.futures import Future
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import wait
from typing import Generic
from typing import TypeVar

from aeris.behavior import Behavior
from aeris.behavior import get_loops
from aeris.exchange import Exchange
from aeris.identifier import AgentIdentifier

BehaviorT = TypeVar('BehaviorT', bound=Behavior)


class Agent(Generic[BehaviorT]):
    """Executable agent.

    An agent executes predefined [`Behavior`][aeris.behavior.Behavior]. An
    agent can operate independently or as part of a broader multi-agent
    system.

    Args:
        behavior: Behavior that the agent will exhibit.
        aid: Identifier of this agent in a multi-agent system.
        exchange: Message exchange of multi-agent system.
    """

    def __init__(
        self,
        behavior: BehaviorT,
        *,
        aid: AgentIdentifier | None = None,
        exchange: Exchange | None = None,
    ) -> None:
        self.aid = aid
        self.behavior = behavior
        self.exchange = exchange
        self.done = threading.Event()
        self._futures: tuple[Future[None], ...] | None = None
        self._start_loops_lock = threading.Lock()

    def __call__(self) -> None:
        """Alias for [run()][aeris.agent.Agent.run]."""
        self.run()

    def run(self) -> None:
        """Run the agent.

        1. Calls [`Behavior.setup()`][aeris.behavior.Behavior.setup].
        1. Starts threads for all control loops defined on the agent's
           [`Behavior`][aeris.behavior.Behavior].
        1. Starts a thread for listening to messages from the
           [`Exchange`][aeris.exchange.Exchange] (if provided).
        1. Waits for the threads to exit.
        1. Calls [`Behavior.shutdown()`][aeris.behavior.Behavior.shutdown].
        """
        self.behavior.setup()

        futures: list[Future[None]] = []
        with ThreadPoolExecutor() as pool:
            with self._start_loops_lock:
                for method in get_loops(self.behavior).values():
                    futures.append(pool.submit(method, self.done))

                self._futures = tuple(futures)

            for future in as_completed(futures):
                future.result()

        self.behavior.shutdown()

    def shutdown(self, timeout: float | None = None) -> None:
        """Shutdown control loops.

        Sets the shutdown event passed to each control loop method and waits
        for the threads to exit.

        Args:
            timeout: How long to wait for threads to exit gracefully.
        """
        self.done.set()

        with self._start_loops_lock:
            if self._futures is None:
                return

            wait(self._futures, timeout=timeout)
