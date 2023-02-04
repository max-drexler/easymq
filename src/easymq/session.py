"""
easymq.session
~~~~~~~~~~~~~~

This module contains objects and functions to maintain a long-term amqp session.
"""

from typing import List

import pika

from .connection import ServerConnection, get_connection_params

_CURRENT_SESSION = None


class AmqpSession:
    def __init__(self) -> None:
        self._connections: List[ServerConnection] = []

    def __del__(self) -> None:
        pass

    def disconnect(self, *args) -> None:
        if not args:
            [x.close() for x in self._connections]
        else:
            for serv in args:
                try:
                    con_index = self._connections.index(serv)
                except ValueError:
                    continue
                connection = self._connections.pop(con_index)
                connection.close()

    def connect(self, *args, credentials=None, add=False) -> None:
        for serv in args:
            try:
                if serv in self._connections:
                    continue
                self._connections.append(
                    ServerConnection(get_connection_params(host=serv))
                )
            except Exception as e:
                self.disconnect()
                raise e


def get_current_session():
    global _CURRENT_SESSION
    if _CURRENT_SESSION is None:
        _CURRENT_SESSION = AmqpSession()
    return _CURRENT_SESSION
