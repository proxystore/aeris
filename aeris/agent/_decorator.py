from __future__ import annotations

import inspect
from typing import Callable
from typing import overload
from typing import ParamSpec
from typing import TypeVar

from aeris.agent._protocol import Agent
from aeris.agent._protocol import ControlLoop

P = ParamSpec('P')
T = TypeVar('T')
R = TypeVar('R')


@overload
def agent(
    cls: type[Agent],
    /,
) -> type[Agent]: ...


@overload
def agent(
    cls: None = None,
    /,
) -> Callable[[type[Agent]], type[Agent]]: ...


def agent(
    cls: type[Agent] | None = None,
    /,
) -> type[Agent] | Callable[[type[Agent]], type[Agent]]:
    def decorator(cls: type[Agent]) -> type[Agent]:
        return cls

    if cls is None:
        return decorator
    else:
        return decorator(cls)


def action(method: Callable[P, R]) -> Callable[P, R]:
    method._actor_method_type = 'action'  # type: ignore[attr-defined]
    return method


def loop(method: Callable[P, R]) -> Callable[P, R]:
    method._actor_method_type = 'loop'  # type: ignore[attr-defined]

    found_sig = inspect.signature(method)
    expected_sig = inspect.signature(ControlLoop.__call__)

    if found_sig != expected_sig:
        raise TypeError(
            f'Signature of loop method "{method.__name__}" is {found_sig} '
            f'but should be {expected_sig}.',
        )

    return method
