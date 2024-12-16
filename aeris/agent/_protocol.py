from __future__ import annotations

from typing import Any
from typing import Dict  # noqa: UP035
from typing import Generic
from typing import Literal
from typing import ParamSpec
from typing import Protocol
from typing import runtime_checkable
from typing import TypeVar

T_co = TypeVar('T_co', bound=type, covariant=True)
P = ParamSpec('P')
R_co = TypeVar('R_co', covariant=True)


@runtime_checkable
class Agent(Protocol[T_co]):
    __agent_actions__: Actions
    __agent_loops__: ControlLoops

    def setup(self) -> None: ...

    def shutdown(self) -> None: ...


class Action(Generic[P, R_co], Protocol):
    _agent_method_type: Literal['action'] = 'action'

    def __call__(self, *arg: P.args, **kwargs: P.kwargs) -> R_co: ...


Actions = Dict[str, Action[Any, Any]]  # noqa: UP006


class ControlLoop(Generic[P, R_co], Protocol):
    _agent_method_type: Literal['loop'] = 'loop'

    def __call__(self) -> R_co: ...


ControlLoops = Dict[str, ControlLoop[Any, Any]]  # noqa: UP006
