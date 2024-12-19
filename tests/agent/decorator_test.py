from __future__ import annotations

from aeris.agent import action
from aeris.agent import Agent
from aeris.agent import agent
from aeris.agent import loop


@agent
class SimpleAgent: ...


def test_simple_agent_decorator() -> None:
    instance = SimpleAgent()
    assert isinstance(instance, Agent)

    instance.setup()
    instance.shutdown()

    parsed_actions = set(instance.__agent_actions__.keys())
    parsed_loops = set(instance.__agent_loops__.keys())

    assert parsed_actions == set()
    assert parsed_loops == set()


@agent()
class ComplexAgent:
    def setup(self) -> None: ...

    def shutdown(self) -> None: ...

    @action
    def action1(self) -> bool:
        return True

    @action
    def action2(self) -> None: ...

    @loop
    def loop1(self) -> bool:
        return True

    @loop
    def loop2(self) -> None: ...

    def method(self) -> bool:
        return True


def test_complex_agent_decorator() -> None:
    instance = ComplexAgent()
    assert isinstance(instance, Agent)

    instance.setup()
    instance.shutdown()

    # Methods should still be callable and mypy should understand this
    assert instance.method()
    assert instance.action1()
    assert instance.loop1()

    parsed_actions = set(instance.__agent_actions__.keys())
    parsed_loops = set(instance.__agent_loops__.keys())

    assert parsed_actions == {'action1', 'action2'}
    assert parsed_loops == {'loop1', 'loop2'}
