from __future__ import annotations

import threading

from aeris.agent import Agent
from aeris.behavior import Behavior
from aeris.exchange import Exchange
from aeris.handle import Handle
from aeris.identifier import AgentIdentifier


class ThreadLauncher:
    def __init__(self, exchange: Exchange) -> None:
        self._agents: dict[AgentIdentifier, threading.Thread] = {}
        self._exchange = exchange

    def start(self, behavior: Behavior) -> Handle:
        aid = self._exchange.register_agent()

        runner = Agent(behavior, aid=aid, exchange=self._exchange)
        thread = threading.Thread(target=runner)
        thread.start()
        self._agents[aid] = thread

        return self._exchange.create_handle(aid)

    def close(self) -> None:
        for aid in self._agents:
            self._agents[aid].join()
