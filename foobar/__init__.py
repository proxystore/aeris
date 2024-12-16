"""foobar package."""

# It is recommended to not write code in the __init__.py because it is easy
# to introduce import cycles and code becomes harder to search for.
from __future__ import annotations

import importlib.metadata as importlib_metadata
import sys

__version__ = importlib_metadata.version('foobar')
