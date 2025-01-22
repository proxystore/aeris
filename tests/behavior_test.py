from __future__ import annotations

import threading

import pytest

from aeris.behavior import action
from aeris.behavior import Behavior
from aeris.behavior import loop


def test_initialize_base_type_error() -> None:
    with pytest.raises(
        TypeError,
        match='The Behavior type cannot be instantiated directly',
    ):
        Behavior()


class Default(Behavior):
    pass


def test_default_behavior() -> None:
    instance = Default()

    assert isinstance(instance, Default)
    assert isinstance(str(instance), str)
    assert isinstance(repr(instance), str)

    instance.setup()
    instance.shutdown()

    assert len(instance.behavior_actions()) == 0
    assert len(instance.behavior_loops()) == 0


class Complex(Behavior):
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
    instance = Complex()

    assert isinstance(instance, Behavior)
    assert isinstance(str(instance), str)
    assert isinstance(repr(instance), str)

    instance.setup()
    instance.shutdown()

    assert instance.method()
    assert instance.action1()
    instance.loop1(threading.Event())

    actions = instance.behavior_actions()
    assert set(actions) == {'action1', 'action2'}

    loops = instance.behavior_loops()
    assert set(loops) == {'loop1', 'loop2'}


def test_invalid_loop_signature() -> None:
    class Bad(Behavior):
        def loop(self) -> None: ...

    with pytest.raises(TypeError, match='Signature of loop method "loop"'):
        loop(Bad.loop)
