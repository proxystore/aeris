from __future__ import annotations

import threading
from concurrent.futures import as_completed
from concurrent.futures import Future
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import wait
from typing import Any
from typing import Generic
from typing import TypeVar

from aeris.agent._protocol import Action
from aeris.agent._protocol import Agent
from aeris.agent._protocol import ControlLoop

AgentT = TypeVar('AgentT', bound=Agent)


def is_actor_method_type(obj: Any, kind: str) -> bool:
    return (
        callable(obj)
        and hasattr(obj, '_actor_method_type')
        and obj._actor_method_type == kind
    )


def get_actions(agent: AgentT) -> dict[str, Action[Any, Any]]:
    actions: dict[str, Action[Any, Any]] = {}
    for name in dir(agent):
        attr = getattr(agent, name)
        if is_actor_method_type(attr, 'action'):
            actions[name] = attr
    return actions


def get_loops(agent: AgentT) -> dict[str, ControlLoop]:
    loops: dict[str, ControlLoop] = {}
    for name in dir(agent):
        attr = getattr(agent, name)
        if is_actor_method_type(attr, 'loop'):
            loops[name] = attr
    return loops


class AgentRunner(Generic[AgentT]):
    def __init__(self, agent: AgentT) -> None:
        self.agent = agent
        self.done = threading.Event()
        self._futures: tuple[Future[None], ...] | None = None
        self._start_loops_lock = threading.Lock()

    def __call__(self) -> None:
        self.run()

    def run(self) -> None:
        self.agent.setup()

        futures: list[Future[None]] = []
        with ThreadPoolExecutor() as pool:
            with self._start_loops_lock:
                for method in get_loops(self.agent).values():
                    futures.append(pool.submit(method, self.done))

                self._futures = tuple(futures)

            for future in as_completed(futures):
                future.result()

        self.agent.shutdown()

    def shutdown(self, timeout: float | None = None) -> None:
        self.done.set()

        with self._start_loops_lock:
            if self._futures is None:
                return

            wait(self._futures, timeout=timeout)
