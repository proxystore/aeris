from __future__ import annotations

import inspect
import sys
import threading
from typing import Any
from typing import Callable
from typing import Generic
from typing import Literal
from typing import Protocol
from typing import runtime_checkable
from typing import TypeVar

if sys.version_info >= (3, 10):  # pragma: >=3.10 cover
    from typing import ParamSpec
else:  # pragma: <3.10 cover
    from typing_extensions import ParamSpec

T = TypeVar('T')
P = ParamSpec('P')
R = TypeVar('R')
R_co = TypeVar('R_co', covariant=True)


@runtime_checkable
class Behavior(Protocol):
    """Agent behavior protocol.

    This protocol defines the behavior an [`Agent`][aeris.agent.Agent]
    will exhibit. A behavior is composed of three parts:
      1. The [`startup()`][aeris.behavior.Behavior.setup] and
         [`shutdown()`][aeris.behavior.Behavior.shutdown] methods that are
         invoked once and the start and end of an agent's execution,
         respectively. The methods should be used to initialize and cleanup
         stateful resources. Resource initialization should not be performed
         in `__init__`.
      2. Action methods annotated with [`@action`][aeris.behavior.action]
         are methods that other agents can invoke on this agent. An agent
         may also call it's own action methods as normal methods.
      3. Control loop methods annotated with [`@loop`][aeris.behavior.loop]
         are executed in separate threads when the agent is executed.
    """

    def setup(self) -> None:
        """Setup up resources needed for the agents execution.

        This is called before an control loop threads are started.
        """
        ...

    def shutdown(self) -> None:
        """Shutdown resources after the agents execution.

        This is called after control loop threads have exited.
        """
        ...


class Action(Generic[P, R_co], Protocol):
    """Action method protocol."""

    _agent_method_type: Literal['action'] = 'action'

    def __call__(self, *arg: P.args, **kwargs: P.kwargs) -> R_co:
        """Expected signature of methods decorated as an action.

        In general, action methods can implement any signature.
        """
        ...


class ControlLoop(Protocol):
    """Control loop method protocol."""

    _agent_method_type: Literal['loop'] = 'loop'

    def __call__(self, shutdown: threading.Event) -> None:
        """Expected signature of methods decorated as a control loop.

        Args:
            shutdown: Event indicating that the agent has been instructed to
                shutdown and all control loops should exit.

        Returns:
            Control loops should not return anything.
        """
        ...


def action(method: Callable[P, R]) -> Callable[P, R]:
    """Decorator that annotates a method of a behavior as an action.

    Marking a method of a behavior as an action makes the method available
    to other agents. I.e., peers within a multi-agent system can only invoke
    methods marked as actions on each other. This enables behaviors to
    define "private" methods.

    Example:
        ```python
        from aeris.behavior import action

        class ExampleBehavior:
            @action
            def perform(self):
                ...
        ```
    """
    method._actor_method_type = 'action'  # type: ignore[attr-defined]
    return method


def loop(method: Callable[P, R]) -> Callable[P, R]:
    """Decorator that annotates a method of a behavior as a control loop.

    Control loop methods of a behavior are run as threads when an agent
    starts. A control loop can run for a well-defined period of time or
    indefinitely, provided the control loop exits when the `shutdown`
    event, passed as a parameter to all control loop methods, is set.

    Example:
        ```python
        import threading
        from aeris.behavior import loop

        class ExampleBehavior:
            @loop
            def listen(self, shutdown: threading.Event) -> None:
                while not shutdown.is_set():
                    ...
        ```

    Raises:
        TypeError: if the method signature does not conform to the
            [`ControlLoop`][aeris.behavior.ControlLoop] protocol.
    """
    method._actor_method_type = 'loop'  # type: ignore[attr-defined]

    if sys.version_info >= (3, 10):  # pragma: >=3.10 cover
        found_sig = inspect.signature(method, eval_str=True)
        expected_sig = inspect.signature(ControlLoop.__call__, eval_str=True)
    else:  # pragma: <3.10 cover
        found_sig = inspect.signature(method)
        expected_sig = inspect.signature(ControlLoop.__call__)

    if found_sig != expected_sig:
        raise TypeError(
            f'Signature of loop method "{method.__name__}" is {found_sig} '
            f'but should be {expected_sig}. If the signatures look the same '
            'except that types are stringified, try importing '
            '"from __future__ import annotations" at the top of the module '
            'where the behavior is defined.',
        )

    return method


def _is_actor_method_type(obj: Any, kind: str) -> bool:
    return (
        callable(obj)
        and hasattr(obj, '_actor_method_type')
        and obj._actor_method_type == kind
    )


def get_actions(behavior: Behavior) -> dict[str, Action[Any, Any]]:
    """Get methods annotated as actions.

    Args:
        behavior: Behavior instance to get action methods from.

    Returns:
        Dictionary mapping of method names to methods annotated as actions.
    """
    actions: dict[str, Action[Any, Any]] = {}
    for name in dir(behavior):
        attr = getattr(behavior, name)
        if _is_actor_method_type(attr, 'action'):
            actions[name] = attr
    return actions


def get_loops(behavior: Behavior) -> dict[str, ControlLoop]:
    """Get methods annotated as loops.

    Args:
        behavior: Behavior instance to get loop methods from.

    Returns:
        Dictionary mapping of method names to methods annotated as loops.
    """
    loops: dict[str, ControlLoop] = {}
    for name in dir(behavior):
        attr = getattr(behavior, name)
        if _is_actor_method_type(attr, 'loop'):
            loops[name] = attr
    return loops
