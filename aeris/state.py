from __future__ import annotations

import pathlib
import shelve
from typing import Literal
from typing import TypeVar

DEFAULT_PICKLE_PROTOCOL = 5
ValueT = TypeVar('ValueT')


class FileState(shelve.DbfilenameShelf[ValueT]):
    """Dictionary interface for persistent state.

    Persists arbitrary Python objects to disk using [pickle][pickle] and
    a [dbm][dbm] database.

    Note:
        This class uses the [shelve][shelve] module so refer there for
        additional caveats.

    Example:
        ```python
        from typing import Any
        from aeris.behavior import Behavior, action
        from aeris.state import FileState

        class Example(Behavior):
            def __init__(self) -> None:
                self.state_path = '/tmp/agent-state.dbm'

            def on_setup(self) -> None:
                self.state: FileState[Any] = FileState(self.state_path)

            def on_shutdown(self) -> None:
                self.state.close()

            @action
            def get_state(self, key: str) -> Any:
                return self.state[key]

            @action
            def modify_state(self, key: str, value: Any) -> None:
                self.state[key] = value
        ```

    Args:
        filename: Base filename for the underlying databased used to store
            key-value pairs.
        flag: Open an existing database read-only: `r`; open an existing
            database for read and write: `w`; open a database for read and
            write, creating it if not existent: `c` (default); always create
            a new empty database for read and write: `n`.
        protocol: Pickling protocol. Defaults to version 5; `None` uses
            the [pickle][pickle] default version.
        writeback: By default (`False`), modified objects are only written
            when assigned. If `True`, the object will hold a cache of all
            entries accessed and write them back to the dict at sync and close
            times. This allows natural operations on mutable entries, but can
            consume much more memory and make sync and close take a long time.
    """

    def __init__(
        self,
        filename: str | pathlib.Path,
        *,
        flag: Literal['r', 'w', 'c', 'n'] = 'c',
        protocol: int | None = DEFAULT_PICKLE_PROTOCOL,
        writeback: bool = False,
    ) -> None:
        super().__init__(
            str(filename),
            flag=flag,
            protocol=protocol,
            writeback=writeback,
        )
