from __future__ import annotations

import threading
from typing import Generic
from typing import Literal
from typing import ParamSpec
from typing import Protocol
from typing import runtime_checkable
from typing import TypeVar

P = ParamSpec('P')
R_co = TypeVar('R_co', covariant=True)


@runtime_checkable
class Agent(Protocol):
    def setup(self) -> None: ...

    def shutdown(self) -> None: ...


class Action(Generic[P, R_co], Protocol):
    _agent_method_type: Literal['action'] = 'action'

    def __call__(self, *arg: P.args, **kwargs: P.kwargs) -> R_co: ...


class ControlLoop(Protocol):
    _agent_method_type: Literal['loop'] = 'loop'

    def __call__(self, shutdown: threading.Event) -> None: ...
