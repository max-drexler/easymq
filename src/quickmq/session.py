"""
easymq.session
~~~~~~~~~~~~~~

This module contains objects and functions to maintain a long-term amqp session.
"""

from functools import wraps
import logging
from typing import Any, Callable, Iterable, List, Optional, Tuple, Union

from .publish import AmqpPublisher
from .exceptions import NotAuthenticatedError, NotConnectedError
from .connection import ConnectionPool
from .message import Packet, Message
from .config import CURRENT_CONFIG

LOGGER = logging.getLogger('quickmq')


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

    @staticmethod
    def connection_required(func: Callable) -> Callable:
        @wraps(func)
        def check_conn(self, *args: Any, **kwargs: Any) -> Any:
            if len(self.servers) > 0:
                return func(self, *args, **kwargs)

            try:
                self.connect(CURRENT_CONFIG.get("DEFAULT_SERVER"))
                return func(self, *args, **kwargs)
            except (NotAuthenticatedError, ConnectionError, AttributeError) as e:
                LOGGER.critical(f"Error when connecting to default server: {e}")
                raise NotConnectedError(
                    f"Need to be connected to a server,\
    could not connect to default '{CURRENT_CONFIG.get('DEFAULT_SERVER')}"
                )

        return check_conn

    @connection_required
    def publish(
        self,
        message: Union[Message, Any],
        key: Optional[str] = None,
        exchange: Optional[str] = None,
        confirm_delivery=True,
    ):
        pckt = Packet(
            message if isinstance(message, Message) else Message(message),
            key or CURRENT_CONFIG.get("DEFAULT_ROUTE_KEY"),
            exchange or CURRENT_CONFIG.get("DEFAULT_EXCHANGE"),
            confirm=confirm_delivery,
        )
        self._publisher.publish_to_pool(self._connections, pckt)

    @connection_required
    def publish_all(
        self,
        messages: Iterable[Union[str, Tuple[str, Any]]],
        exchange: Optional[str] = None,
        confirm_delivery=True,
    ):
        for val in messages:
            key = None
            msg = None
            if type(val) is tuple:
                key, msg = val
            else:
                msg = val
            self.publish(msg, key, exchange=exchange, confirm_delivery=confirm_delivery)

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
