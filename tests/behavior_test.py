from __future__ import annotations

import threading

import pytest

from aeris.behavior import action
from aeris.behavior import Behavior
from aeris.behavior import get_actions
from aeris.behavior import get_loops
from aeris.behavior import loop


class BasicBehavior:
    def setup(self) -> None: ...

    def shutdown(self) -> None: ...


def test_basic_behavior() -> None:
    instance = BasicBehavior()
    assert isinstance(instance, BasicBehavior)

    instance.setup()
    instance.shutdown()


class ComplexBehavior:
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


def test_complex_behavior() -> None:
    instance = ComplexBehavior()
    assert isinstance(instance, Behavior)

    instance.setup()
    instance.shutdown()

    assert instance.method()
    assert instance.action1()
    instance.loop1(threading.Event())

    actions = get_actions(instance)
    assert set(actions) == {'action1', 'action2'}

    loops = get_loops(instance)
    assert set(loops) == {'loop1', 'loop2'}


def test_invalid_loop_signature() -> None:
    class BadBehavior:
        def setup(self) -> None: ...

        def shutdown(self) -> None: ...

        def loop(self) -> None: ...

    with pytest.raises(TypeError, match='Signature of loop method "loop"'):
        loop(BadBehavior.loop)
