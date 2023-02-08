from contextlib import contextmanager
import socket
import threading
import time
from typing import Any, Callable, List, Optional, Tuple

import pika
from pika.adapters.blocking_connection import BlockingChannel
from pika.exceptions import (
    AMQPConnectionError,
    AuthenticationError,
    ConnectionClosed,
    ProbableAccessDeniedError,
    ProbableAuthenticationError,
)

from .config import (
    RECONNECT_DELAY,
    RECONNECT_TRIES,
    RABBITMQ_PORT,
    DEFAULT_USER,
    DEFAULT_PASS,
)
from .exceptions import NotAuthenticatedError


class ServerConnection(threading.Thread):
    def __init__(
        self,
        host: str,
        port: Optional[int] = None,
        vhost: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ) -> None:
        super().__init__(None, None, f"Thread-MQConnection({host})", None, None)

        self._connection: pika.BlockingConnection = None
        self._channel: BlockingChannel = None
        self._confirmed_channel: BlockingChannel = None
        self._con_params = pika.ConnectionParameters(
            host=host,
            port=port or RABBITMQ_PORT,
            virtual_host=vhost or "/",
            credentials=pika.PlainCredentials(
                username or DEFAULT_USER, password or DEFAULT_PASS
            ),
        )

        self._running = False
        self.connect()
        self.start()

    @property
    def port(self) -> int:
        return self._con_params.port

    @property
    def vhost(self) -> str:
        return self._con_params.virtual_host

    @property
    def server(self) -> str:
        return self._con_params.host

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def connected(self) -> bool:
        return self._connection.is_open

    @contextmanager
    def prepare_connection(self) -> None:
        if not self.is_running:
            raise ConnectionAbortedError("Lost connection to RabbitMQ Server")
        yield
        self._reconnect_channel()

    def run(self) -> None:
        self._running = True
        while self._running:
            try:
                self._connection.process_data_events(time_limit=1)
            except AMQPConnectionError:
                self.close()

    def connect(self) -> None:
        if self._connection is not None and self._connection.is_open:
            self.__channel_setup()
        try:
            self._connection = pika.BlockingConnection(parameters=self._con_params)
            self.__channel_setup()
        except (socket.gaierror, socket.herror):
            raise ConnectionError("Could not connect to server")
        except (
            AuthenticationError,
            ProbableAccessDeniedError,
            ProbableAuthenticationError,
        ):
            raise NotAuthenticatedError("Not authenticated to connect to server")

    def close(self) -> None:
        self._running = False
        if self._connection is None or self._connection.is_closed:
            return
        self._connection.process_data_events(time_limit=1)
        if self._connection.is_open:
            self._connection.close()

    def __channel_setup(self) -> None:
        if self._channel is None or self._channel.is_closed:
            self._channel = self._connection.channel()
        if self._confirmed_channel is None or self._confirmed_channel.is_closed:
            self._confirmed_channel = self._connection.channel()
            self._confirmed_channel.confirm_delivery()

    def add_callback(self, callback: Callable, *args, **kwargs) -> None:
        self._connection.add_callback_threadsafe(lambda: callback(*args, **kwargs))

    def _reconnect_channel(self) -> None:
        self.__channel_setup()

    def __del__(self) -> None:
        self.close()
        self._connection = None

    def __eq__(self, _obj: Any) -> bool:
        if isinstance(_obj, str):
            return _obj == self.server
        return super().__eq__(_obj)

    def __hash__(self) -> int:
        return id(self)

    def __str__(self) -> str:
        return str(self.server)


class ReconnectConnection(ServerConnection):
    def __init__(
        self,
        host: str,
        port: Optional[int] = None,
        vhost: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ) -> None:
        self._reconnecting = threading.Event()
        self._reconnecting.set()  # not currently reconnecting, threads shouldn't wait
        self._reconnecting_callbacks = []

        super().__init__(host, port, vhost, username, password)

    @property
    def is_reconnecting(self) -> bool:
        return not self._reconnecting.is_set()

    def close(self) -> None:
        self._running = False
        self.wait_for_reconnect()
        return super().close()

    def run(self) -> None:
        self._running = True
        while self._running:
            try:
                self._connection.process_data_events(time_limit=1)
            except (AMQPConnectionError, ConnectionError, ConnectionClosed):
                self.__reconnect()

    def __reconnect(self) -> None:
        self._reconnecting.clear()
        tries = RECONNECT_TRIES
        while self._running:
            if tries == 0:
                self._reconnecting.set()
                self.close()
                raise RuntimeWarning(
                    f"Could not reconnect to {self.server} after {RECONNECT_TRIES} attempt(s), exiting..."
                )
            try:
                self.connect()
                if self._connection.is_open:
                    break
            except AMQPConnectionError:
                pass
            if self._running:
                time.sleep(RECONNECT_DELAY)
            if tries < 0:
                continue
            tries -= 1
        self._reconnecting.set()
        self.__process_blocked_callbacks()

    def __process_blocked_callbacks(self) -> None:
        while len(self._reconnecting_callbacks) > 0:
            callback, args, kwargs = self._reconnecting_callbacks.pop()
            self._connection.call_later(0, lambda: callback(*args, **kwargs))

    def add_callback(self, callback: Callable, *args, **kwargs) -> None:
        if self.is_reconnecting:
            self._reconnecting_callbacks.insert(0, (callback, args, kwargs))
        else:
            super().add_callback(callback, args, kwargs)

    def wait_for_reconnect(self, timeout=None) -> bool:
        self._reconnecting.wait(timeout=timeout)
        return self.is_reconnecting

    def _reconnect_channel(self) -> None:
        self.wait_for_reconnect()
        self.__channel_setup()

    def prepare_connection(self) -> None:
        self.wait_for_reconnect()
        return super().prepare_connection()


class ConnectionPool:
    def __init__(self) -> None:
        self._connections: List[ServerConnection] = []

    def _remove_connection(self, server: str) -> None:
        try:
            con_index = self._connections.index(server)
        except ValueError:
            return
        connection = self._connections.pop(con_index)
        connection.close()

    def add_server(self, new_server: str, auth: Tuple[str, str] = (None, None)) -> None:
        self._remove_connection(new_server)  # Remove connection if it already exists
        self._connections.append(
            ReconnectConnection(
                host=new_server,
                username=auth[0] or DEFAULT_USER,
                password=auth[1] or DEFAULT_PASS,
            )
        )

    def add_connection(self, new_conn: ServerConnection) -> None:
        self._remove_connection(new_conn.server)
        self._connections.append(new_conn)

    def remove_server(self, server: str) -> None:
        self._remove_connection(server)

    def remove_all(self) -> None:
        [self._remove_connection(con.server) for con in self._connections]
