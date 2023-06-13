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

LOGGER = logging.getLogger("quickmq")


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
                f"Need to be connected to a server, "
                f"could not connect to default '{CURRENT_CONFIG.get('DEFAULT_SERVER')}"
            )

    return check_conn


class AmqpSession:
    def __init__(self) -> None:
        self._connection_pool = ConnectionPool()
        self._publisher = AmqpPublisher()

    @property
    def servers(self) -> List[str]:
        return [con.server for con in self._connection_pool]

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
        self._publisher.publish_to_pool(self._connection_pool, pckt)

    @connection_required
    def publish_all(
        self,
        messages: Iterable[Union[Any, Tuple[str, Any]]],
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

    def disconnect(self, *servers) -> None:
        if not servers:
            self._connection_pool.remove_all()
        else:
            for serv in servers:
                self._connection_pool.remove_server(serv)

    def connect(
        self, *servers, auth: Tuple[Optional[str], Optional[str]] = (None, None)
    ) -> None:
        for server in servers:
            try:
                self._connection_pool.add_server(server, auth=auth)
            except (NotAuthenticatedError, ConnectionError):
                raise

    def __str__(self) -> str:
        return f"[Amqp Session] connected to: {', '.join(self.servers)}"

    def __enter__(self):
        return self

    def __exit__(self, *args) -> None:
        self.disconnect()
