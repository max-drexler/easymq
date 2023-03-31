"""
quickmq.editor
~~~~~~~~~~~~~~

Contains classes and methods to edit a RabbitMQ server's topology.
"""

from enum import Enum
import logging
from typing import Optional

from quickmq.connection import ConnectionPool

LOG = logging.getLogger("quickmq")


class ExchangeType(Enum):
    DIRECT = "direct"
    TOPIC = "topic"
    FANOUT = "fanout"
    HEADER = "header"


class TopologyEditor:
    def __init__(self, pool: ConnectionPool) -> None:
        self._pool = pool

    def exchange_declare(
        self,
        exchange: str,
        exchange_type: ExchangeType,
        durable=False,
        auto_delete=False,
        internal=False,
    ) -> None:
        pass

    def exchange_bind(
        self, destination: str, source: str, route_key: Optional[str] = None
    ) -> None:
        pass

    def exchange_unbind(
        self, destination: str, source: str, route_key: Optional[str] = None
    ) -> None:
        pass

    def __exchange_check():
        pass

    def exchange_check(
        self,
        exchange: str,
        exchange_type=ExchangeType.DIRECT,
        durable=False,
        auto_delete=False,
        internal=False,
    ) -> bool:
        pass

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
        pass

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
