"""
quickmq.editor
~~~~~~~~~~~~~~

Contains classes and methods to edit a RabbitMQ server's topology.
"""

from enum import Enum
import logging
from typing import Generator, Optional

from .connection import ConnectionPool, ServerConnection
from .exceptions import ExchangeExistsError, FailedEditError, ExchangeNotExistError
from .structures import ConnectionSyncMixin


LOG = logging.getLogger("quickmq")


class ExchangeType(Enum):
    DIRECT = "direct"
    TOPIC = "topic"
    FANOUT = "fanout"
    HEADER = "header"


class TopologyEditor(ConnectionSyncMixin):
    def __init__(self, pool: ConnectionPool) -> None:
        super().__init__()
        self._pool = pool

    def __call_connection__(
        self, connection: ServerConnection, chan_method: str, *args, **kwargs
    ) -> None:
        with self.sync_to_caller(), connection.wrapper():
            pika_method = getattr(connection._confirmed_channel, chan_method, None)
            if pika_method is None:
                raise FailedEditError(
                    f"Method {chan_method} doesn't exist! This shouldn't happen."
                )
            pika_method(*args, **kwargs)

    def __servers_from_arg(
        self, server: Optional[str]
    ) -> Generator[ServerConnection, None, None]:
        if server is None:
            yield from self._pool
        else:
            serv_con = self._pool.get_connection(server)
            if serv_con is None:
                raise ValueError(f"Not connected to server {server}!")
            yield serv_con

    def exchange_declare(
        self,
        exchange: str,
        exchange_type=ExchangeType.DIRECT,
        durable=False,
        auto_delete=False,
        internal=False,
        server: Optional[str] = None,
    ) -> None:
        if self.exchange_check(exchange, server=server):
            raise ExchangeExistsError(exchange=exchange)
        for serv in self.__servers_from_arg(server):
            with self.sync_to_connection():
                serv.add_callback(
                    self.__call_connection__,
                    serv,
                    "exchange_declare",
                    exchange=exchange,
                    exchange_type=ExchangeType(exchange_type).value,
                    durable=durable,
                    auto_delete=auto_delete,
                    internal=internal,
                )

    def exchange_bind(
        self,
        destination: str,
        source: str,
        route_key: Optional[str] = None,
        server: Optional[str] = None,
    ) -> None:
        if not self.exchange_check(destination, server=server):
            raise ExchangeNotExistError(exchange=destination)
        if not self.exchange_check(source, server=server):
            raise ExchangeExistsError(exchange=source)

    def exchange_unbind(
        self, destination: str, source: str, route_key: Optional[str] = None
    ) -> None:
        pass

    def exchange_check(self, exchange: str, server: Optional[str] = None) -> bool:
        return False

    def exchange_delete(self, exchange: str, if_unused=False) -> None:
        pass

    def queue_declare(
        self, queue: str, durable=False, exclusive=False, auto_delete=False
    ) -> None:
        pass

    def queue_delete(self, queue: str, if_unused=False, if_empty=False) -> None:
        pass

    def queue_check(
        self, queue: str, durable=False, exclusive=False, auto_delete=False
    ) -> bool:
        return False

    def queue_purge(self, queue: str) -> None:
        pass

    def queue_bind(
        self, queue: str, exchange: str, route_key: Optional[str] = None
    ) -> None:
        pass

    def queue_unbind(
        self,
        queue: str,
        exchange: Optional[str] = None,
        route_key: Optional[str] = None,
    ) -> None:
        pass
