"""
easymq.session
~~~~~~~~~~~~~~

This module contains objects and functions to maintain a long-term amqp session.
"""

import atexit
import logging
from typing import Any, Callable, Iterable, List, Optional, Tuple, Union

from .publish import AmqpPublisher
from .exceptions import NotAuthenticatedError, NotConnectedError
from .connection import ConnectionPool
from .message import Packet, Message
from .config import CURRENT_CONFIG

LOGGER = logging.getLogger(__name__)
_CURRENT_SESSION = None


def exit_handler():
    if _CURRENT_SESSION is None:
        return
    _CURRENT_SESSION.disconnect()


atexit.register(exit_handler)


def get_current_session():
    global _CURRENT_SESSION
    if _CURRENT_SESSION is None:
        _CURRENT_SESSION = AmqpSession()
    return _CURRENT_SESSION


def connection_required(func: Callable) -> Callable:
    def check_conn(*args, **kwargs):
        if len(get_current_session().servers) > 0:
            func(*args, **kwargs)
            return

        try:
            get_current_session().connect(CURRENT_CONFIG.get("DEFAULT_SERVER"))
            func(*args, **kwargs)
            return
        except (NotAuthenticatedError, ConnectionError, AttributeError) as e:
            LOGGER.critical(f"Error when connecting to default server: {e}")
            raise NotConnectedError(
                f"Need to be connected to a server,\
could not connect to default '{CURRENT_CONFIG.get('DEFAULT_SERVER')}"
            )

    return check_conn


class AmqpSession:
    def __init__(self) -> None:
        self._connections = ConnectionPool()
        self._publisher = AmqpPublisher()

    @property
    def servers(self) -> List[str]:
        return [con.server for con in self._connections]

    @property
    def pool(self) -> ConnectionPool:
        return self._connections

    @connection_required
    def publish(
        self,
        message: Union[Message, Any],
        key: Optional[str] = None,
        exchange: Optional[str] = None,
        block=False,
    ):
        pckt = Packet(
            message if isinstance(message, Message) else Message(message),
            key or CURRENT_CONFIG.get("DEFAULT_ROUTE_KEY"),
            exchange or CURRENT_CONFIG.get("DEFAULT_EXCHANGE"),
            confirm=block,
        )
        self._publisher.publish_to_pool(self._connections, pckt)

    @connection_required
    def publish_all(
        self,
        messages: Iterable[Union[str, Tuple[str, str]]],
        exchange: Optional[str] = None,
        block=False,
    ):
        for val in messages:
            key = None
            msg = None
            if type(val) is tuple:
                msg, key = val
            else:
                msg = val
            self.publish(msg, key, exchange=exchange, block=block)

    def disconnect(self, *args) -> None:
        if not args:
            self._connections.remove_all()
        else:
            for serv in args:
                self._connections.remove_server(serv)

    def connect(
        self, *args, auth: Tuple[Optional[str], Optional[str]] = (None, None)
    ) -> None:
        for server in args:
            try:
                self._connections.add_server(server, auth=auth)
            except (NotAuthenticatedError, ConnectionError):
                raise

    def __str__(self) -> str:
        return f"[Amqp Session] connected to: {', '.join(self.servers)}"

    def __del__(self) -> None:
        self.disconnect()
        del self._connections

    def __enter__(self):
        return self

    def __exit__(self, *args) -> None:
        self.disconnect()
