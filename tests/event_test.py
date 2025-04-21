from __future__ import annotations

import threading

from academy.event import or_event


def test_or_event() -> None:
    a = threading.Event()
    b = threading.Event()

    c = or_event(a, b)

    assert not c.is_set()
    a.set()
    assert c.is_set()
    a.clear()
    assert not c.is_set()

    a.set()
    b.set()
    assert c.is_set()

    # Both events must be cleared
    a.clear()
    assert c.is_set()
    b.clear()
    assert not c.is_set()
