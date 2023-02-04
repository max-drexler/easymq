import socket
import threading
import time
from typing import Any, Callable

import pika
from pika.adapters.blocking_connection import BlockingChannel
from pika.exceptions import (
    AMQPConnectionError,
    AuthenticationError,
    ConnectionClosed,
    ProbableAccessDeniedError,
    ProbableAuthenticationError,
)

from .config import RECONNECT_DELAY, RECONNECT_TRIES, DEFAULT_SERVER, RABBITMQ_PORT
from .adapter import MQCredentials

def get_connection_params(
    host=None,
    port=None,
    vhost=None,
    credentials=None,
) -> pika.ConnectionParameters:
    return pika.ConnectionParameters(
        blocked_connection_timeout=None,
        client_properties=None,
        credentials=credentials or MQCredentials().creds,
        heartbeat=None,
        host=host or DEFAULT_SERVER,
        port=port or RABBITMQ_PORT,
        socket_timeout=None,
        ssl_options=None,
        virtual_host=vhost or '/',
    )


class ServerConnection(threading.Thread):
    def __init__(
        self,
        connection_params: pika.ConnectionParameters,
    ) -> None:
        super().__init__(
            None, None, f"Thread-MQConnection({connection_params.host})", None, None
        )
        self._con_params = connection_params

        self._running = False
        self._reconnecting = threading.Event()
        self._reconnecting.set()  # not currently reconnecting, threads shouldn't wait

        self._connection: pika.BlockingConnection = None
        self._channel: BlockingChannel = None
        self._confirmed_channel: BlockingChannel = None

        self.connect()
        self.start()

    @property
    def port(self) -> int:
        return self._con_params.port

    @property
    def is_reconnecting(self) -> bool:
        return not self._reconnecting.is_set()

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def server(self) -> str:
        return self._con_params.host

    def run(self) -> None:
        self._running = True
        while self._running:
            try:
                self._connection.process_data_events(time_limit=1)
            except ConnectionClosed:
                self.__reconnect()
            except AMQPConnectionError:
                self.__reconnect()

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
            raise AuthenticationError("Not authenticated to connect to server")

    def close(self) -> None:
        self._running = False
        self.wait_for_reconnect()
        if self._connection is None or self._connection.is_closed:
            return
        self._connection.process_data_events(time_limit=1)
        if self._connection.is_open:
            self._connection.close()

    def _reconnect_channel(self) -> None:
        self.wait_for_reconnect()
        self.__channel_setup()

    def wait_for_reconnect(self, timeout=None) -> bool:
        self._reconnecting.wait(timeout=timeout)
        return self.is_reconnecting

    def __channel_setup(self) -> None:
        if self._channel is None or self._channel.is_closed:
            self._channel = self._connection.channel()
        if self._confirmed_channel is None or self._confirmed_channel.is_closed:
            self._confirmed_channel = self._connection.channel()
            self._confirmed_channel.confirm_delivery()

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

    def add_callback(self, callback: Callable, *args, **kwargs) -> None:
        self._connection.add_callback_threadsafe(lambda: callback(*args, **kwargs))

    def __del__(self) -> None:
        self.close()
        self._connection = None

    def __eq__(self, _obj: Any) -> bool:
        if isinstance(_obj, str):
            return _obj == self.server
        elif isinstance(_obj, ServerConnection):
            return id(self) == id(_obj)
        else:
            return False

    def __hash__(self) -> int:
        return id(self)
