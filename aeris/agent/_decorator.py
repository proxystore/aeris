from __future__ import annotations

from typing import Any
from typing import Callable
from typing import cast
from typing import overload
from typing import ParamSpec
from typing import TypeVar

from aeris.agent._protocol import Actions
from aeris.agent._protocol import Agent
from aeris.agent._protocol import ControlLoops

T = TypeVar('T', bound=type)
P = ParamSpec('P')
R = TypeVar('R')


def is_actor_method_type(obj: Any, kind: str) -> bool:
    return (
        callable(obj)
        and hasattr(obj, '_actor_method_type')
        and obj._actor_method_type == kind
    )


def get_actions(cls: T) -> Actions:
    actions: Actions = {}
    for name in dir(cls):
        attr = getattr(cls, name)
        if is_actor_method_type(attr, 'action'):
            actions[name] = attr
    return actions


def get_loops(cls: T) -> ControlLoops:
    loops: ControlLoops = {}
    for name in dir(cls):
        attr = getattr(cls, name)
        if is_actor_method_type(attr, 'loop'):
            loops[name] = attr
    return loops


def _default_setup_shutdown(*args: Any, **kwargs: Any) -> None:
    pass


@overload
def agent(
    cls: T,
    /,
) -> Agent[T]: ...


@overload
def agent(
    cls: None = None,
    /,
) -> Callable[[T], Agent[T]]: ...


def agent(
    cls: T | None = None,
    /,
) -> Agent[T] | Callable[[T], Agent[T]]:
    def decorator(cls: T) -> Agent[T]:
        cls.__agent_actions__ = get_actions(cls)  # type: ignore[attr-defined]
        cls.__agent_loops__ = get_loops(cls)  # type: ignore[attr-defined]

        if not hasattr(cls, 'setup'):
            cls.setup = _default_setup_shutdown  # type: ignore[attr-defined]
        if not hasattr(cls, 'shutdown'):
            cls.shutdown = _default_setup_shutdown  # type: ignore[attr-defined]

        return cast(Agent[T], cls)

    if cls is None:
        return decorator
    else:
        return decorator(cls)


def action(method: Callable[P, R]) -> Callable[P, R]:
    method._actor_method_type = 'action'  # type: ignore[attr-defined]
    return method


def loop(method: Callable[P, R]) -> Callable[P, R]:
    method._actor_method_type = 'loop'  # type: ignore[attr-defined]
    return method
