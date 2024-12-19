from __future__ import annotations

import threading
from typing import Any
from typing import Generic
from typing import NamedTuple
from typing import TypeVar

from aeris.agent import Agent
from aeris.behavior import Behavior
from aeris.exchange import Exchange
from aeris.handle import Handle
from aeris.identifier import AgentIdentifier

BehaviorT = TypeVar('BehaviorT', bound=Behavior)


class _RunningAgent(NamedTuple, Generic[BehaviorT]):
    agent: Agent[BehaviorT]
    thread: threading.Thread


class ThreadLauncher:
    def __init__(self, exchange: Exchange) -> None:
        self._agents: dict[AgentIdentifier, _RunningAgent[Any]] = {}
        self._exchange = exchange

    def start(self, behavior: Behavior) -> Handle:
        aid = self._exchange.register_agent()

        agent = Agent(behavior, aid=aid, exchange=self._exchange)
        thread = threading.Thread(target=agent)
        thread.start()
        self._agents[aid] = _RunningAgent(agent, thread)

        return self._exchange.create_handle(aid)

    def close(self) -> None:
        for aid in self._agents:
            self._agents[aid].agent.shutdown()
        for aid in self._agents:
            self._agents[aid].thread.join()
