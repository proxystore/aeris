from __future__ import annotations

from concurrent.futures import Future
from typing import Any
from typing import TYPE_CHECKING
from typing import TypeVar

from aeris.identifier import AgentIdentifier

if TYPE_CHECKING:
    from aeris.exchange import Exchange

T = TypeVar('T')


class Handle:
    """Client handle to a running agent.

    A handle enables a client to invoke actions on an agent.

    Args:
        aid: Identifier of the agent.
        exchange: Message exchange used to communicate with agent.
    """

    def __init__(self, aid: AgentIdentifier, exchange: Exchange) -> None:
        self.aid = aid
        self.exchange = exchange

    def invoke(
        self,
        action: str,
        /,
        *args: Any,
        **kwargs: Any,
    ) -> Future[T]:
        """Invoke an action on the agent.

        Args:
            action: Action to invoke.
            args: Positional arguments for the action.
            kwargs: Keywords arguments for the action.

        Returns:
            Future to the result of the action.
        """
        raise NotImplementedError
