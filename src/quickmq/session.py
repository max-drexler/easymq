"""
easymq.session
~~~~~~~~~~~~~~

This module contains objects and functions to maintain a long-term amqp session.
"""

import logging
from typing import Any, List, Optional, Tuple, Union

from .publish import AmqpPublisher
from .connection import ConnectionPool
from .message import Packet, Message
from .config import CURRENT_CONFIG
from .editor import TopologyEditor

LOGGER = logging.getLogger("quickmq")


class AmqpClient:
    def __init__(self) -> None:
        self._connection_pool = ConnectionPool()
        self._publisher = AmqpPublisher()
        self._consumer = None

    @property
    def servers(self) -> List[str]:
        return [con.server for con in self._connection_pool]

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
            self._connection_pool.add_server(server, auth=auth)

    def publish(
        self,
        message: Union[Message, Any],
        key: Optional[str] = None,
        exchange: Optional[str] = None,
        confirm_delivery=True,
    ):
        if len(self._connection_pool) == 0:
            raise ConnectionError("Must be connected to a server to publish!")
        pckt = Packet(
            message if isinstance(message, Message) else Message(message),
            key or CURRENT_CONFIG.get("DEFAULT_ROUTE_KEY"),
            exchange or CURRENT_CONFIG.get("DEFAULT_EXCHANGE"),
            confirm=confirm_delivery,
        )
        self._publisher.publish_to_pool(self._connection_pool, pckt)

    def edit(self) -> TopologyEditor:
        if len(self._connection_pool) == 0:
            raise ConnectionError("Must be connected to a server to edit!")
        return TopologyEditor(self._connection_pool)

    def __str__(self) -> str:
        return f"[Amqp Session] connected to: {', '.join(self.servers)}"

    def __repr__(self) -> str:
        return f"<AMQPSession>[{','.join(self.servers)}]"

    def __enter__(self):
        return self

    def __exit__(self, *args) -> None:
        self.disconnect()
