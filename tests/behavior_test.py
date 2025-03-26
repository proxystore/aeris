from __future__ import annotations

import threading

import pytest

from aeris.behavior import Behavior
from aeris.behavior import loop
from aeris.handle import Handle
from aeris.handle import HandleDict
from aeris.handle import HandleList
from aeris.handle import ProxyHandle
from testing.behavior import EmptyBehavior
from testing.behavior import HandleBehavior
from testing.behavior import IdentityBehavior
from testing.behavior import WaitBehavior


def test_initialize_base_type_error() -> None:
    error = 'The Behavior type cannot be instantiated directly'
    with pytest.raises(TypeError, match=error):
        Behavior()


def test_behavior_empty() -> None:
    behavior = EmptyBehavior()
    behavior.on_setup()

    assert isinstance(behavior, EmptyBehavior)
    assert isinstance(str(behavior), str)
    assert isinstance(repr(behavior), str)

    assert len(behavior.behavior_actions()) == 0
    assert len(behavior.behavior_loops()) == 0
    assert len(behavior.behavior_handles()) == 0

    behavior.on_shutdown()


def test_behavior_actions() -> None:
    behavior = IdentityBehavior()
    behavior.on_setup()

    actions = behavior.behavior_actions()
    assert set(actions) == {'identity'}

    assert behavior.identity(1) == 1

    behavior.on_shutdown()


def test_behavior_loops() -> None:
    behavior = WaitBehavior()
    behavior.on_setup()

    loops = behavior.behavior_loops()
    assert set(loops) == {'wait'}

    shutdown = threading.Event()
    shutdown.set()
    behavior.wait(shutdown)

    behavior.on_shutdown()


def test_behavior_handles() -> None:
    handle = ProxyHandle(EmptyBehavior())
    behavior = HandleBehavior(handle)
    behavior.on_setup()

    handles = behavior.behavior_handles()
    assert set(handles) == {'handle'}

    behavior.on_shutdown()


def test_behavior_handles_bind() -> None:
    class _TestBehavior(Behavior):
        def __init__(self, handle: Handle[EmptyBehavior]) -> None:
            self.direct = handle
            self.sequence = HandleList([handle])
            self.mapping = HandleDict({'x': handle})

    expected_binds = 3
    bind_count = 0

    def _bind_handle(handle: Handle[EmptyBehavior]) -> Handle[EmptyBehavior]:
        nonlocal bind_count
        bind_count += 1
        return handle

    handle = ProxyHandle(EmptyBehavior())
    behavior = _TestBehavior(handle)
    behavior.on_setup()

    behavior.behavior_handles_bind(_bind_handle)
    assert bind_count == expected_binds

    behavior.on_shutdown()


def test_invalid_loop_signature() -> None:
    class BadBehavior(Behavior):
        def loop(self) -> None: ...

    with pytest.raises(TypeError, match='Signature of loop method "loop"'):
        loop(BadBehavior.loop)
