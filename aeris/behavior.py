from __future__ import annotations

import inspect
import threading
from typing import Any
from typing import Callable
from typing import Generic
from typing import Literal
from typing import ParamSpec
from typing import Protocol
from typing import runtime_checkable
from typing import TypeVar

T = TypeVar('T')
P = ParamSpec('P')
R = TypeVar('R')
R_co = TypeVar('R_co', covariant=True)


@runtime_checkable
class Behavior(Protocol):
    def setup(self) -> None: ...

    def shutdown(self) -> None: ...


class Action(Generic[P, R_co], Protocol):
    _agent_method_type: Literal['action'] = 'action'

    def __call__(self, *arg: P.args, **kwargs: P.kwargs) -> R_co: ...


class ControlLoop(Protocol):
    _agent_method_type: Literal['loop'] = 'loop'

    def __call__(self, shutdown: threading.Event) -> None: ...


def action(method: Callable[P, R]) -> Callable[P, R]:
    method._actor_method_type = 'action'  # type: ignore[attr-defined]
    return method


def loop(method: Callable[P, R]) -> Callable[P, R]:
    method._actor_method_type = 'loop'  # type: ignore[attr-defined]

    found_sig = inspect.signature(method, eval_str=True)
    expected_sig = inspect.signature(ControlLoop.__call__, eval_str=True)

    if found_sig != expected_sig:
        raise TypeError(
            f'Signature of loop method "{method.__name__}" is {found_sig} '
            f'but should be {expected_sig}.',
        )

    return method


def _is_actor_method_type(obj: Any, kind: str) -> bool:
    return (
        callable(obj)
        and hasattr(obj, '_actor_method_type')
        and obj._actor_method_type == kind
    )


def get_actions(behavior: Behavior) -> dict[str, Action[Any, Any]]:
    actions: dict[str, Action[Any, Any]] = {}
    for name in dir(behavior):
        attr = getattr(behavior, name)
        if _is_actor_method_type(attr, 'action'):
            actions[name] = attr
    return actions


def get_loops(behavior: Behavior) -> dict[str, ControlLoop]:
    loops: dict[str, ControlLoop] = {}
    for name in dir(behavior):
        attr = getattr(behavior, name)
        if _is_actor_method_type(attr, 'loop'):
            loops[name] = attr
    return loops
