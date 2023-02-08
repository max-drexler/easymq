"""
easymq.session
~~~~~~~~~~~~~~

This module contains objects and functions to maintain a long-term amqp session.
"""

import warnings
from typing import List, Optional, Tuple

from .exceptions import NotAuthenticatedError, NotConnectedError
from .connection import ConnectionPool, ServerConnection, ReconnectConnection
from .config import DEFAULT_SERVER

_CURRENT_SESSION = None


def get_current_session():
    global _CURRENT_SESSION
    if _CURRENT_SESSION is None:
        _CURRENT_SESSION = AmqpSession()
    return _CURRENT_SESSION


class AmqpSession:

    def __init__(self) -> None:
        self._connections = ConnectionPool()
        try:
            self.connect(DEFAULT_SERVER)
        except (NotAuthenticatedError, ConnectionError):
            warnings.warn(f"Couldn't connect to default server {DEFAULT_SERVER}")

    def disconnect(self, *args) -> None:
        if not args:
            self._connections.remove_all()
        else:
            [self._connections.remove_server(serv) for serv in args]

    def connect(self, *args, auth: Tuple[Optional[str], Optional[str]] = (None, None)) -> None:
        for server in args:
            try:
                self._connections.add_server(server, auth=auth)
            except (NotAuthenticatedError, ConnectionError):
                raise

    @property
    def connections(self) -> List[ServerConnection]:
        return self._connections

    @property
    def connections_required(self) -> List[ServerConnection]:
        if not self._connections:
            raise NotConnectedError("Not currently connected to any servers, call easymq.connect()")
        return self.connections

    def wait_for_reconnect(self, timeout=None) -> None:
        for server in self._connections:
            if isinstance(server, ReconnectConnection):
                server.wait_for_reconnect(timeout=timeout)

    def __str__(self) -> str:
        return f"[Amqp Session] connected to: {', '.join(self._connections)}"

    def __del__(self) -> None:
        self.disconnect()
        self._connections = None

    def __enter__(self):
        return self

    def __exit__(self, *args) -> None:
        self.disconnect()
