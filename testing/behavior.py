from __future__ import annotations

import threading
import time
from typing import TypeVar

from academy.behavior import action
from academy.behavior import Behavior
from academy.behavior import loop
from academy.handle import Handle

T = TypeVar('T')


class EmptyBehavior(Behavior):
    pass


class ErrorBehavior(Behavior):
    @action
    def fails(self) -> None:
        raise RuntimeError('This action always fails.')


class HandleBehavior(Behavior):
    def __init__(self, handle: Handle[EmptyBehavior]) -> None:
        self.handle = handle


class IdentityBehavior(Behavior):
    @action
    def identity(self, value: T) -> T:
        return value


class WaitBehavior(Behavior):
    @loop
    def wait(self, shutdown: threading.Event) -> None:
        shutdown.wait()


class CounterBehavior(Behavior):
    def __init__(self) -> None:
        self._count = 0

    def on_setup(self) -> None:
        self._count = 0

    @action
    def add(self, value: int) -> None:
        self._count += value

    @action
    def count(self) -> int:
        return self._count


class SleepBehavior(Behavior):
    def __init__(self, loop_sleep: float = 0.001) -> None:
        self.loop_sleep = loop_sleep
        self.steps = 0

    def on_shutdown(self) -> None:
        assert self.steps > 0

    @action
    def sleep(self, sleep: float) -> None:
        time.sleep(sleep)

    @loop
    def count(self, shutdown: threading.Event) -> None:
        while not shutdown.is_set():
            time.sleep(self.loop_sleep)
            self.steps += 1
