from contextlib import contextmanager
import queue
import socket
import threading
import time
from typing import Any, Callable, List, Optional, Tuple
import logging

import pika
from pika.adapters.blocking_connection import BlockingChannel
from pika.exceptions import (
    AMQPConnectionError,
    AuthenticationError,
    ConnectionClosed,
    ProbableAccessDeniedError,
    ProbableAuthenticationError,
    StreamLostError,
)

from .config import CURRENT_CONFIG
from .exceptions import NotAuthenticatedError

LOGGER = logging.getLogger('quickmq')


class ServerConnection(threading.Thread):
    def __init__(
        self,
        host: str,
        port: Optional[int] = None,
        vhost: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ) -> None:
        super().__init__(
            None, None, f"Thread-MQConnection({host})", (), {}, daemon=True
        )

        self._running = False
        self._callback_queue: queue.Queue = queue.Queue()
        self._connection: Optional[pika.BlockingConnection] = None
        self._channel: Optional[BlockingChannel] = None
        self._confirmed_channel: BlockingChannel = None
        self._con_params = pika.ConnectionParameters(
            host=host,
            port=port or CURRENT_CONFIG.get("RABBITMQ_PORT"),
            virtual_host=vhost or "/",
            credentials=pika.PlainCredentials(
                username or CURRENT_CONFIG.get("DEFAULT_USER"),
                password or CURRENT_CONFIG.get("DEFAULT_PASS"),
            ),
        )
        LOGGER.info(
            f"Setting up connection to '{host}' with username \
'{self._con_params.credentials.username}' on port \
'{self._con_params.port}' and vhost '{self._con_params.virtual_host}'"
        )
        self.connect()
        self.start()

    @property
    def user(self) -> str:
        return self._con_params.credentials.username

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
    def connected(self) -> bool:
        return self._running and not self._connection.is_closed

    @contextmanager
    def wrapper(self):
        if not self._running:
            LOGGER.error(f'Connection to {self.server} closed')
            raise ConnectionAbortedError(f"Connection to {self.server} closed")
        try:
            yield
        finally:
            if self._channel.is_closed or self._confirmed_channel.is_closed:
                LOGGER.info(f'Re-establishing channels on connection to {self.server}')
                self._channel_setup()

    def run(self) -> None:
        self._running = True
        while self._running:
            try:
                self._connection.process_data_events(.005)
            except (
                StreamLostError,
                AMQPConnectionError,
                ConnectionError,
                ConnectionClosed,
            ) as e:
                self._on_connection_error(e)
            try:
                callback, args, kwargs = self._callback_queue.get_nowait()
                callback(*args, **kwargs)
            except queue.Empty:
                continue
            except TypeError as e:
                LOGGER.warning(f"Callback has wrong method signature: {e}")

    def _on_connection_error(self, exception: BaseException) -> None:
        LOGGER.error(
            f"Error in connection to {self.server}: {exception}, closing connection"
        )
        self.close()

    def connect(self) -> None:
        if self._connection is not None and self._connection.is_open:
            self._channel_setup()
        try:
            self._connection = pika.BlockingConnection(parameters=self._con_params)
            self._channel_setup()
        except (socket.gaierror, socket.herror) as e:
            LOGGER.error(f"Socket error connecting to {self.server}: {e}")
            raise ConnectionError("Could not connect to server")
        except (
            AuthenticationError,
            ProbableAccessDeniedError,
            ProbableAuthenticationError,
        ):
            LOGGER.error(
                f"Not authenticated to connect to {self.server} with username {self.user}"
            )
            raise NotAuthenticatedError(
                f"Not authenticated to connect to server {self.server}"
            )
        LOGGER.info(f"Connection established to {self.server}")

    def _close(self) -> None:
        self._connection.process_data_events()
        if self._connection is not None and self._connection.is_open:
            self._connection.close()
        LOGGER.info(f'Closed connection to {self.server}')

    def close(self) -> None:
        LOGGER.info(f"Closing connection to {self.server}")
        self._running = False
        if self._connection is None or self._connection.is_closed:
            LOGGER.info(f'Closed connection to {self.server}')
            return
        self.add_callback(self._close)

    def _channel_setup(self) -> None:
        if self._channel is None or self._channel.is_closed:
            self._channel = self._connection.channel()
        if self._confirmed_channel is None or self._confirmed_channel.is_closed:
            self._confirmed_channel = self._connection.channel()
            self._confirmed_channel.confirm_delivery()

    def add_callback(self, callback: Callable, *args, **kwargs) -> None:
        LOGGER.debug(
            f"Adding callback on {self.server}: {callback}, args: {args}, kwargs: {kwargs}"
        )
        LOGGER.debug(f'Currently {self._callback_queue.qsize()} callbacks in queue for {self.server}')
        self._callback_queue.put((callback, args, kwargs))

    def __del__(self) -> None:
        self.close()
        del self._connection

    def __eq__(self, _obj: Any) -> bool:
        if isinstance(_obj, str):
            return _obj == self.server
        return super().__eq__(_obj)

    def __hash__(self) -> int:
        return id(self)

    def __str__(self) -> str:
        return f'Connection to {self.server}'

    def __repr__(self) -> str:
        return f'<ServerConnection({self.server}, {self.user}, {self.port}, {self.vhost}, connected?{self.connected})>'


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

        super().__init__(host, port, vhost, username, password)

    @property
    def is_reconnecting(self) -> bool:
        return not self._reconnecting.is_set()

    def close(self) -> None:
        self._running = False
        self.wait_for_reconnect()
        return super().close()

    def _on_connection_error(self, exception: BaseException) -> None:
        self.__reconnect()

    def __reconnect(self) -> None:
        LOGGER.info(f'Attempting reconnect to {self.server}')
        self._reconnecting.clear()
        tries = CURRENT_CONFIG.get("RECONNECT_TRIES")
        while self._running:
            if tries == 0:
                self._reconnecting.set()
                self.close()
                LOGGER.critical(
                    f"Could not reconnect to {self.server} after \
                    {CURRENT_CONFIG.get('RECONNECT_TRIES')} attempt(s), exiting..."
                )
                raise RuntimeWarning(
                    f"Could not reconnect to {self.server} after \
                    {CURRENT_CONFIG.get('RECONNECT_TRIES')} attempt(s), exiting..."
                )
            try:
                self.connect()
                if self._connection.is_open:
                    break
            except AMQPConnectionError:
                pass
            if self._running:
                LOGGER.debug(
                    f"Waiting {CURRENT_CONFIG.get('RECONNECT_DELAY')} seconds before next reconnect attempt"
                )
                time.sleep(CURRENT_CONFIG.get("RECONNECT_DELAY"))
            if tries < 0:
                continue
            tries -= 1
            LOGGER.debug(f"{tries} more reconnect attempts")
        LOGGER.info(f'Reconnect finished to {self.server}')
        self._reconnecting.set()

    def wait_for_reconnect(self, timeout=None) -> bool:
        self._reconnecting.wait(timeout=timeout)
        LOGGER.info(f'Connection to {self.server} reconnecting? {self.is_reconnecting}')
        return self.is_reconnecting


class ConnectionPool:
    def __init__(self) -> None:
        self._connections: List[ServerConnection] = []

    @property
    def connections(self) -> List[ServerConnection]:
        return self._connections

    def _remove_connection(self, server: str) -> None:
        try:
            con_index = self._connections.index(server)
        except ValueError:
            LOGGER.debug(f"{server} not in current connection pool")
            return
        connection = self._connections.pop(con_index)
        connection.close()

    def add_server(
        self, new_server: str, auth: Tuple[Optional[str], Optional[str]] = (None, None)
    ) -> None:
        self._remove_connection(new_server)  # Remove connection if it already exists
        self._connections.append(
            ReconnectConnection(
                host=new_server,
                username=auth[0],
                password=auth[1],
            )
        )

    def add_connection(self, new_conn: ServerConnection) -> None:
        self._remove_connection(new_conn.server)
        self._connections.append(new_conn)

    def remove_server(self, server: str) -> None:
        self._remove_connection(server)

    def remove_all(self) -> None:
        for con in self._connections:
            self._remove_connection(con.server)

    def __len__(self) -> int:
        return len(self._connections)

    def __iter__(self):
        return iter(self._connections)

    def add_callback(self, callback, *args, **kwargs) -> None:
        for con in self._connections:
            con.add_callback(callback, (con,) + args, **kwargs)
