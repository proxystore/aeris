from __future__ import annotations

import threading

import pytest

from aeris.agent import action
from aeris.agent import Agent
from aeris.agent import agent
from aeris.agent import loop


@agent
class SimpleAgent:
    def setup(self) -> None: ...

    def shutdown(self) -> None: ...


def test_simple_agent_decorator() -> None:
    instance = SimpleAgent()
    assert isinstance(instance, Agent)

    instance.setup()
    instance.shutdown()


@agent
class ComplexAgent:
    def setup(self) -> None: ...

    def shutdown(self) -> None: ...

    @action
    def action1(self) -> bool:
        return True

    @action
    def action2(self) -> None: ...

    @loop
    def loop1(self, shutdown: threading.Event) -> None: ...

    @loop
    def loop2(self, shutdown: threading.Event) -> None: ...

    def method(self) -> bool:
        return True


def test_complex_agent_decorator() -> None:
    instance = ComplexAgent()
    assert isinstance(instance, Agent)

    instance.setup()
    instance.shutdown()

    assert instance.method()
    assert instance.action1()
    instance.loop1(threading.Event())


def test_invalid_loop_signature() -> None:
    @agent
    class BadAgent:
        def setup(self) -> None: ...

        def shutdown(self) -> None: ...

        def loop(self) -> None: ...

    with pytest.raises(TypeError, match='Signature of loop method "loop"'):
        loop(BadAgent.loop)
