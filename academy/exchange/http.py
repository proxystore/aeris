"""HTTP message exchange client and server.

To start the exchange:
```bash
python -m academy.exchange.http --host localhost --port 1234
```

Connect to the exchange through the client.
```python
from academy.exchange.http import HttpExchange

with HttpExchange('localhost', 1234) as exchange:
    aid = exchange.register_agent()
    mailbox = exchange.get_mailbox(aid)
    ...
    mailbox.close()
```
"""

from __future__ import annotations

import argparse
import contextlib
import logging
import multiprocessing
import sys
import uuid
from collections.abc import AsyncGenerator
from collections.abc import Generator
from collections.abc import Sequence
from typing import Any
from typing import TypeVar

if sys.version_info >= (3, 11):  # pragma: >=3.11 cover
    from typing import Self
else:  # pragma: <3.11 cover
    from typing_extensions import Self

import requests
from aiohttp.web import AppKey
from aiohttp.web import Application
from aiohttp.web import AppRunner
from aiohttp.web import json_response
from aiohttp.web import Request
from aiohttp.web import Response
from aiohttp.web import run_app
from aiohttp.web import TCPSite
from pydantic import TypeAdapter
from pydantic import ValidationError

from academy.behavior import Behavior
from academy.exception import BadEntityIdError
from academy.exception import MailboxClosedError
from academy.exchange import ExchangeMixin
from academy.exchange.queue import AsyncQueue
from academy.exchange.queue import QueueClosedError
from academy.identifier import AgentId
from academy.identifier import ClientId
from academy.identifier import EntityId
from academy.logging import init_logging
from academy.message import BaseMessage
from academy.message import Message
from academy.socket import wait_connection

logger = logging.getLogger(__name__)

BehaviorT = TypeVar('BehaviorT', bound=Behavior)

_OKAY_CODE = 200
_BAD_REQUEST_CODE = 400
_FORBIDDEN_CODE = 403
_NOT_FOUND_CODE = 404


class HttpExchange(ExchangeMixin):
    """Http exchange client.

    Args:
        hostname: Host name of the exchange server.
        port: Port of the exchange server.
    """

    def __init__(
        self,
        host: str,
        port: int,
    ) -> None:
        self.host = host
        self.port = port

        self._session = requests.Session()
        self._mailbox_url = f'http://{self.host}:{self.port}/mailbox'
        self._message_url = f'http://{self.host}:{self.port}/message'
        self._discover_url = f'http://{self.host}:{self.port}/discover'

    def __reduce__(
        self,
    ) -> tuple[type[Self], tuple[str, int]]:
        return (type(self), (self.host, self.port))

    def __repr__(self) -> str:
        return f'{type(self).__name__}(host="{self.host}", port={self.port})'

    def __str__(self) -> str:
        return f'{type(self).__name__}<{self.host}:{self.port}>'

    def close(self) -> None:
        """Close this exchange client."""
        self._session.close()
        logger.debug('Closed exchange (%s)', self)

    def register_agent(
        self,
        behavior: type[BehaviorT],
        *,
        agent_id: AgentId[BehaviorT] | None = None,
        name: str | None = None,
    ) -> AgentId[BehaviorT]:
        """Create a new agent identifier and associated mailbox.

        Args:
            behavior: Type of the behavior this agent will implement.
            agent_id: Specify the ID of the agent. Randomly generated
                default.
            name: Optional human-readable name for the agent. Ignored if
                `agent_id` is provided.

        Returns:
            Unique identifier for the agent's mailbox.
        """
        aid = AgentId.new(name=name) if agent_id is None else agent_id
        response = self._session.post(
            self._mailbox_url,
            json={
                'mailbox': aid.model_dump_json(),
                'behavior': ','.join(behavior.behavior_mro()),
            },
        )
        response.raise_for_status()
        logger.debug('Registered %s in %s', aid, self)
        return aid

    def register_client(
        self,
        *,
        name: str | None = None,
    ) -> ClientId:
        """Create a new client identifier and associated mailbox.

        Args:
            name: Optional human-readable name for the client.

        Returns:
            Unique identifier for the client's mailbox.
        """
        cid = ClientId.new(name=name)
        response = self._session.post(
            self._mailbox_url,
            json={'mailbox': cid.model_dump_json()},
        )
        response.raise_for_status()
        logger.debug('Registered %s in %s', cid, self)
        return cid

    def terminate(self, uid: EntityId) -> None:
        """Close the mailbox for an entity from the exchange.

        Note:
            This method is a no-op if the mailbox does not exists.

        Args:
            uid: Entity identifier of the mailbox to close.
        """
        response = self._session.delete(
            self._mailbox_url,
            json={'mailbox': uid.model_dump_json()},
        )
        response.raise_for_status()
        logger.debug('Closed mailbox for %s (%s)', uid, self)

    def discover(
        self,
        behavior: type[Behavior],
        *,
        allow_subclasses: bool = True,
    ) -> tuple[AgentId[Any], ...]:
        """Discover peer agents with a given behavior.

        Warning:
            Discoverability is not implemented on the HTTP exchange.

        Args:
            behavior: Behavior type of interest.
            allow_subclasses: Return agents implementing subclasses of the
                behavior.

        Returns:
            Tuple of agent IDs implementing the behavior.
        """
        behavior_str = f'{behavior.__module__}.{behavior.__name__}'
        response = self._session.get(
            self._discover_url,
            json={
                'behavior': behavior_str,
                'allow_subclasses': allow_subclasses,
            },
        )
        response.raise_for_status()
        agent_ids = [
            aid
            for aid in response.json()['agent_ids'].split(',')
            if len(aid) > 0
        ]
        return tuple(AgentId(uid=uuid.UUID(aid)) for aid in agent_ids)

    def get_mailbox(self, uid: EntityId) -> HttpMailbox:
        """Get a client to a specific mailbox.

        Args:
            uid: EntityId of the mailbox.

        Returns:
            Mailbox client.

        Raises:
            BadEntityIdError: if a mailbox for `uid` does not exist.
        """
        return HttpMailbox(uid, self)

    def send(self, uid: EntityId, message: Message) -> None:
        """Send a message to a mailbox.

        Args:
            uid: Destination address of the message.
            message: Message to send.

        Raises:
            BadEntityIdError: if a mailbox for `uid` does not exist.
            MailboxClosedError: if the mailbox was closed.
        """
        response = self._session.put(
            self._message_url,
            json={'message': message.model_dump_json()},
        )
        if response.status_code == _NOT_FOUND_CODE:
            raise BadEntityIdError(uid)
        elif response.status_code == _FORBIDDEN_CODE:
            raise MailboxClosedError(uid)
        response.raise_for_status()
        logger.debug('Sent %s to %s', type(message).__name__, uid)


class HttpMailbox:
    """Client interface to a mailbox hosted in an HTTP exchange.

    Args:
        uid: EntityId of the mailbox.
        exchange: Exchange client.

    Raises:
        BadEntityIdError: if a mailbox with `uid` does not exist.
    """

    def __init__(
        self,
        uid: EntityId,
        exchange: HttpExchange,
    ) -> None:
        self._uid = uid
        self._exchange = exchange

        response = self.exchange._session.get(
            self.exchange._mailbox_url,
            json={'mailbox': uid.model_dump_json()},
        )
        response.raise_for_status()
        data = response.json()
        if not data['exists']:
            raise BadEntityIdError(uid)

    @property
    def exchange(self) -> HttpExchange:
        """Exchange client."""
        return self._exchange

    @property
    def mailbox_id(self) -> EntityId:
        """Mailbox address/identifier."""
        return self._uid

    def close(self) -> None:
        """Close this mailbox client.

        Warning:
            This does not close the mailbox in the exchange. I.e., the exchange
            will still accept new messages to this mailbox, but this client
            will no longer be listening for them.
        """
        pass

    def recv(self, timeout: float | None = None) -> Message:
        """Receive the next message in the mailbox.

        This blocks until the next message is received or the mailbox
        is closed.

        Args:
            timeout: Optional timeout in seconds to wait for the next
                message. If `None`, the default, block forever until the
                next message or the mailbox is closed.

        Raises:
            MailboxClosedError: if the mailbox was closed.
            TimeoutError: if a `timeout` was specified and exceeded.
        """
        try:
            response = self.exchange._session.get(
                self.exchange._message_url,
                json={'mailbox': self.mailbox_id.model_dump_json()},
                timeout=timeout,
            )
        except requests.exceptions.Timeout as e:
            raise TimeoutError(
                f'Failed to receive response in {timeout} seconds.',
            ) from e
        if response.status_code == _FORBIDDEN_CODE:
            raise MailboxClosedError(self.mailbox_id)
        response.raise_for_status()

        message = BaseMessage.model_from_json(response.json().get('message'))
        logger.debug(
            'Received %s to %s',
            type(response).__name__,
            self.mailbox_id,
        )
        return message


class _MailboxManager:
    def __init__(self) -> None:
        self._mailboxes: dict[EntityId, AsyncQueue[Message]] = {}
        self._behaviors: dict[AgentId[Any], tuple[str, ...]] = {}

    def check_mailbox(self, uid: EntityId) -> bool:
        return uid in self._mailboxes

    def create_mailbox(
        self,
        uid: EntityId,
        behavior: tuple[str, ...] | None = None,
    ) -> None:
        if uid not in self._mailboxes or self._mailboxes[uid].closed():
            self._mailboxes[uid] = AsyncQueue()
            if behavior is not None and isinstance(uid, AgentId):
                self._behaviors[uid] = behavior
            logger.info('Created mailbox for %s', uid)

    async def terminate(self, uid: EntityId) -> None:
        mailbox = self._mailboxes.get(uid, None)
        if mailbox is not None:
            await mailbox.close()
            logger.info('Closed mailbox for %s', uid)

    async def discover(
        self,
        behavior: str,
        allow_subclasses: bool,
    ) -> list[AgentId[Any]]:
        found: list[AgentId[Any]] = []
        for aid, behaviors in self._behaviors.items():
            if self._mailboxes[aid].closed():
                continue
            if behavior == behaviors[0] or (
                allow_subclasses and behavior in behaviors
            ):
                found.append(aid)
        return found

    async def get(self, uid: EntityId) -> Message:
        try:
            return await self._mailboxes[uid].get()
        except KeyError as e:
            raise BadEntityIdError(uid) from e
        except QueueClosedError as e:
            raise MailboxClosedError(uid) from e

    async def put(self, message: Message) -> None:
        try:
            await self._mailboxes[message.dest].put(message)
        except KeyError as e:
            raise BadEntityIdError(message.dest) from e
        except QueueClosedError as e:
            raise MailboxClosedError(message.dest) from e


MANAGER_KEY = AppKey('manager', _MailboxManager)


async def _create_mailbox_route(request: Request) -> Response:
    data = await request.json()
    manager: _MailboxManager = request.app[MANAGER_KEY]

    try:
        raw_mailbox_id = data['mailbox']
        mailbox_id: EntityId = TypeAdapter(EntityId).validate_json(
            raw_mailbox_id,
        )
        behavior_raw = data.get('behavior', None)
        behavior = (
            behavior_raw.split(',') if behavior_raw is not None else None
        )
    except (KeyError, ValidationError):
        return Response(
            status=_BAD_REQUEST_CODE,
            text='Missing or invalid mailbox ID',
        )

    manager.create_mailbox(mailbox_id, behavior)
    return Response(status=_OKAY_CODE)


async def _terminate_route(request: Request) -> Response:
    data = await request.json()
    manager: _MailboxManager = request.app[MANAGER_KEY]

    try:
        raw_mailbox_id = data['mailbox']
        mailbox_id: EntityId = TypeAdapter(EntityId).validate_json(
            raw_mailbox_id,
        )
    except (KeyError, ValidationError):
        return Response(
            status=_BAD_REQUEST_CODE,
            text='Missing or invalid mailbox ID',
        )

    await manager.terminate(mailbox_id)
    return Response(status=_OKAY_CODE)


async def _discover_route(request: Request) -> Response:
    data = await request.json()
    manager: _MailboxManager = request.app[MANAGER_KEY]

    try:
        behavior = data['behavior']
        allow_subclasses = data['allow_subclasses']
    except (KeyError, ValidationError):
        return Response(
            status=_BAD_REQUEST_CODE,
            text='Missing or invalid arguments',
        )

    agent_ids = await manager.discover(behavior, allow_subclasses)
    return json_response(
        {'agent_ids': ','.join(str(aid.uid) for aid in agent_ids)},
    )


async def _check_mailbox_route(request: Request) -> Response:
    data = await request.json()
    manager: _MailboxManager = request.app[MANAGER_KEY]

    try:
        raw_mailbox_id = data['mailbox']
        mailbox_id: EntityId = TypeAdapter(EntityId).validate_json(
            raw_mailbox_id,
        )
    except (KeyError, ValidationError):
        return Response(
            status=_BAD_REQUEST_CODE,
            text='Missing or invalid mailbox ID',
        )

    exists = manager.check_mailbox(mailbox_id)
    return json_response({'exists': exists})


async def _send_message_route(request: Request) -> Response:
    data = await request.json()
    manager: _MailboxManager = request.app[MANAGER_KEY]

    try:
        raw_message = data.get('message')
        message = BaseMessage.model_from_json(raw_message)
    except (KeyError, ValidationError):
        return Response(
            status=_BAD_REQUEST_CODE,
            text='Missing or invalid message',
        )

    try:
        await manager.put(message)
    except BadEntityIdError:
        return Response(status=_NOT_FOUND_CODE, text='Unknown mailbox ID')
    except MailboxClosedError:
        return Response(status=_FORBIDDEN_CODE, text='Mailbox was closed')
    else:
        return Response(status=_OKAY_CODE)


async def _recv_message_route(request: Request) -> Response:
    data = await request.json()
    manager: _MailboxManager = request.app[MANAGER_KEY]

    try:
        raw_mailbox_id = data['mailbox']
        mailbox_id: EntityId = TypeAdapter(EntityId).validate_json(
            raw_mailbox_id,
        )
    except (KeyError, ValidationError):
        return Response(
            status=_BAD_REQUEST_CODE,
            text='Missing or invalid mailbox ID',
        )

    try:
        message = await manager.get(mailbox_id)
    except BadEntityIdError:
        return Response(status=_NOT_FOUND_CODE, text='Unknown mailbox ID')
    except MailboxClosedError:
        return Response(status=_FORBIDDEN_CODE, text='Mailbox was closed')
    else:
        return json_response({'message': message.model_dump_json()})


def create_app() -> Application:
    """Create a new server application."""
    manager = _MailboxManager()
    app = Application()
    app[MANAGER_KEY] = manager

    app.router.add_post('/mailbox', _create_mailbox_route)
    app.router.add_delete('/mailbox', _terminate_route)
    app.router.add_get('/mailbox', _check_mailbox_route)
    app.router.add_put('/message', _send_message_route)
    app.router.add_get('/message', _recv_message_route)
    app.router.add_get('/discover', _discover_route)

    return app


@contextlib.asynccontextmanager
async def serve_app(
    app: Application,
    host: str,
    port: int,
) -> AsyncGenerator[None]:
    """Serve an application as a context manager.

    Args:
        app: Application to run.
        host: Host to bind to.
        port: Port to bind to.
    """
    runner = AppRunner(app)
    try:
        await runner.setup()
        site = TCPSite(runner, host, port)
        await site.start()
        logger.info('Exchange listening on %s:%s (ctrl-C to exit)', host, port)
        yield
    finally:
        await runner.cleanup()
        logger.info('Exchange closed!')


def _run(
    host: str,
    port: int,
    *,
    level: str | int = logging.INFO,
    logfile: str | None = None,
) -> None:
    app = create_app()
    init_logging(level, logfile=logfile)
    logger = logging.getLogger('root')
    logger.info('Exchange listening on %s:%s (ctrl-C to exit)', host, port)
    run_app(app, host=host, port=port, print=None)
    logger.info('Exchange closed!')


@contextlib.contextmanager
def spawn_http_exchange(
    host: str = '0.0.0.0',
    port: int = 5463,
    *,
    level: int | str = logging.WARNING,
    timeout: float | None = None,
) -> Generator[HttpExchange]:
    """Context manager that spawns an HTTP exchange in a subprocess.

    This function spawns a new process (rather than forking) and wait to
    return until a connection with the exchange has been established.
    When exiting the context manager, `SIGINT` will be sent to the exchange
    process. If the process does not exit within 5 seconds, it will be
    killed.

    Args:
        host: Host the exchange should listen on.
        port: Port the exchange should listen on.
        level: Logging level.
        timeout: Connection timeout when waiting for exchange to start.

    Returns:
        Exchange interface connected to the spawned exchange.
    """
    # Fork is not safe in multi-threaded context.
    multiprocessing.set_start_method('spawn')

    exchange_process = multiprocessing.Process(
        target=_run,
        args=(host, port),
        kwargs={'level': level},
    )
    exchange_process.start()

    logger.info('Starting exchange server...')
    wait_connection(host, port, timeout=timeout)
    logger.info('Started exchange server!')

    try:
        with HttpExchange(host, port) as exchange:
            yield exchange
    finally:
        logger.info('Terminating exchange server...')
        wait = 5
        exchange_process.terminate()
        exchange_process.join(timeout=wait)
        if exchange_process.exitcode is None:  # pragma: no cover
            logger.info(
                'Killing exchange server after waiting %s seconds',
                wait,
            )
            exchange_process.kill()
        else:
            logger.info('Terminated exchange server!')
        exchange_process.close()


def _main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', default='0.0.0.0')
    parser.add_argument('--port', type=int, required=True)
    parser.add_argument('--log-level', default='INFO')

    argv = sys.argv[1:] if argv is None else argv
    args = parser.parse_args(argv)

    _run(args.host, args.port, level=args.log_level)

    return 0


if __name__ == '__main__':
    raise SystemExit(_main())
