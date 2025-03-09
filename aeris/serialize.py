from __future__ import annotations

import pickle
from typing import Any


class NoPickleMixin:
    """Mixin that raises an error if a type is pickled."""

    def __getstate__(self) -> Any:
        raise pickle.PicklingError(f'{type(self).__name__} is not pickleable.')
