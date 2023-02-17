from contextlib import contextmanager
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
    StreamLostError
)

from .config import CURRENT_CONFIG
from .exceptions import NotAuthenticatedError

LOGGER = logging.getLogger(__name__)


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
        return self.is_running and not self._connection.is_closed

    @contextmanager
    def prepare_connection(self):
        if not self.is_running:
            raise ConnectionAbortedError("Lost connection to RabbitMQ Server")
        yield
        self._reconnect_channel()

    def run(self) -> None:
        self._running = True
        while self._running:
            try:
                self._connection.process_data_events(time_limit=1)
            except (StreamLostError, AMQPConnectionError, ConnectionError) as e:
                LOGGER.error(
                    f"Error in connection to {self.server}: {e}, closing connection"
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
                f"Not authenticated to connect to {self.server} with username {self._con_params.credentials.username}"
            )
            raise NotAuthenticatedError(
                f"Not authenticated to connect to server {self.server}"
            )

    def _close(self) -> None:
        self._connection.process_data_events()
        if self._connection.is_open:
            self._connection.close()

    def close(self) -> None:
        LOGGER.debug(f"Closing connection to {self.server}")
        self._running = False
        if self._connection is None or self._connection.is_closed:
            return
        self.add_callback(self._close)

    def _channel_setup(self) -> None:
        if self._channel is None or self._channel.is_closed:
            self._channel = self._connection.channel()
        if self._confirmed_channel is None or self._confirmed_channel.is_closed:
            self._confirmed_channel = self._connection.channel()
            self._confirmed_channel.confirm_delivery()

    def add_callback(self, callback: Callable, *args, **kwargs) -> None:
        self._connection.add_callback_threadsafe(lambda: callback(*args, **kwargs))

    def _reconnect_channel(self) -> None:
        self._channel_setup()

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
        self._reconnecting_callbacks: List[Tuple[Callable, Any, Any]] = []

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
            except (AMQPConnectionError, ConnectionError, ConnectionClosed) as e:
                LOGGER.warning(
                    f"Error in connection to {self.server}: '{e}', reconnecting..."
                )
                self.__reconnect()

    def __reconnect(self) -> None:
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
        self._reconnecting.set()
        self.__process_blocked_callbacks()

    def __process_blocked_callbacks(self) -> None:
        while len(self._reconnecting_callbacks) > 0:
            callback, args, kwargs = self._reconnecting_callbacks.pop()
            _ = self._connection.call_later(0, lambda: callback(*args, **kwargs))

    def add_callback(self, callback: Callable, *args, **kwargs) -> None:
        if self.is_reconnecting:
            self._reconnecting_callbacks.insert(0, (callback, args, kwargs))
        else:
            super().add_callback(callback, *args, **kwargs)

    def wait_for_reconnect(self, timeout=None) -> bool:
        self._reconnecting.wait(timeout=timeout)
        return self.is_reconnecting

    def _reconnect_channel(self) -> None:
        self.wait_for_reconnect()
        self._channel_setup()

    def prepare_connection(self):
        self.wait_for_reconnect()
        return super().prepare_connection()  # need this return


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
