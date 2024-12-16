from __future__ import annotations

import pytest

from foobar.foo import bar
from testing.data import DATA


def test_bar_none_input() -> None:
    assert bar() is None
    assert bar(None) is None


@pytest.mark.parametrize(
    ('data', 'total'),
    (
        ([1], 1),
        ([1, 2], 3),
        ([1, 2, 3], 6),
    ),
)
def test_bar_sum(data: list[int], total: int) -> None:
    assert bar(data) == total

    d = DATA[0]
    assert bar(d[0]) == d[1]
