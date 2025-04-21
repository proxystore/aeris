from __future__ import annotations

import pathlib

from academy.behavior import action
from academy.behavior import Behavior
from academy.state import FileState


class _StatefulBehavior(Behavior):
    def __init__(self, state_path: pathlib.Path) -> None:
        self.state_path = state_path

    def on_setup(self) -> None:
        self.state: FileState[str] = FileState(self.state_path)

    def on_shutdown(self) -> None:
        self.state.close()

    @action
    def get_state(self, key: str) -> str:
        return self.state[key]

    @action
    def modify_state(self, key: str, value: str) -> None:
        self.state[key] = value


def test_file_state(tmp_path: pathlib.Path) -> None:
    state_path = tmp_path / 'state.dbm'

    behavior = _StatefulBehavior(state_path)
    behavior.on_setup()
    key, value = 'foo', 'bar'
    behavior.modify_state(key, value)
    assert behavior.get_state(key) == value
    behavior.on_shutdown()

    behavior = _StatefulBehavior(state_path)
    behavior.on_setup()
    assert behavior.get_state(key) == value
    behavior.on_shutdown()
