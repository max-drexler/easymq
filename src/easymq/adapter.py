import atexit
import socket
import threading
import time
from contextlib import contextmanager
from typing import Callable, Optional, Tuple

import pika
from pika.adapters.blocking_connection import BlockingChannel
from pika.exceptions import (
    AMQPConnectionError,
    AuthenticationError,
    ChannelClosedByBroker,
    ConnectionClosed,
    ProbableAccessDeniedError,
    ProbableAuthenticationError,
)

from easymq.config import RECONNECT_DELAY, RECONNECT_TRIES, DEFAULT_PASS, DEFAULT_USER


class MQCredentials:
    def __init__(
        self,
        username: Optional[str] = None,
        password: Optional[str] = None,
        auth_file: Optional[str] = None,
    ) -> None:
        if auth_file is not None:
            with open(auth_file, "r", encoding="utf-8") as creds:
                username: str = creds.readline().strip()
                __pswd: str = creds.readline().strip()
        else:
            username = username or DEFAULT_USER
            __pswd = password or DEFAULT_PASS

        self._credentials = pika.PlainCredentials(username, __pswd)

    @property
    def creds(self) -> pika.PlainCredentials:
        return self._credentials

    def __eq__(self, other) -> bool:
        if not isinstance(other, MQCredentials):
            return False
        other: MQCredentials
        return (
            self.creds.username == other.creds.username
            and self.creds.password == other.creds.password
        )

    def __hash__(self) -> int:
        return id(self)


class PikaConnection(threading.Thread):
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


class PikaClient:
    def __init__(
        self,
        credentials: Optional[MQCredentials] = None,
        server: str = "localhost",
        port: int = 5672,
    ) -> None:
        self._credentials: pika.PlainCredentials = getattr(
            credentials, "creds", MQCredentials().creds
        )
        self._server_conn = PikaConnection(
            pika.ConnectionParameters(
                host=server, credentials=self._credentials, port=port
            )
        )
        atexit.register(self._server_conn.close)

        self._processing = threading.Event()
        self._processing_error = None

    @property
    def connection(self) -> PikaConnection:
        return self._server_conn

    @property
    def server(self) -> str:
        return self.connection.server

    @contextmanager
    def _sync_caller(self) -> None:
        try:
            self._server_conn.wait_for_reconnect()
            yield
        except ChannelClosedByBroker as e:
            self._processing_error = e
            self._server_conn._reconnect_channel()
        except Exception as e:
            self._processing_error = e
        finally:
            self._processing.set()

    @contextmanager
    def sync_connection(self) -> None:
        try:
            self._server_conn.wait_for_reconnect()
            yield
        finally:
            self._wait_for_processing()

    def check_exchange(self, new_exchange: str) -> bool:
        self._server_conn.wait_for_reconnect()
        self._server_conn.add_callback(
            self.__declare_exchange, new_exchange, passive=True
        )
        try:
            self._wait_for_processing()
        except Exception:
            return False
        return True

    def __declare_exchange(
        self,
        exchange_name: str,
        exchange_type: str = "direct",
        durable=False,
        auto_delete=False,
        internal=False,
        passive=False,
    ) -> None:
        with self._sync_caller():
            self._server_conn._channel.exchange_declare(
                exchange_name,
                exchange_type=exchange_type,
                durable=durable,
                auto_delete=auto_delete,
                internal=internal,
                passive=passive,
            )

    def exchange_declare(
        self,
        exchange_name: str,
        exchange_type: str = "direct",
        durable=False,
        auto_delete=False,
        internal=False,
    ) -> None:
        with self.sync_connection():
            self._server_conn.add_callback(
                self.__declare_exchange,
                str(exchange_name),
                exchange_type=exchange_type,
                durable=durable,
                auto_delete=auto_delete,
                internal=internal,
            )

    def __declare_queue(
        self,
        queue_name: str,
        passive=False,
        durable=False,
        exclusive=False,
        auto_delete=False,
    ) -> None:
        with self._sync_caller():
            self._server_conn._channel.queue_declare(
                queue_name,
                passive=passive,
                durable=durable,
                exclusive=exclusive,
                auto_delete=auto_delete,
            )

    def queue_declare(
        self, queue_name: str, durable=False, exclusive=False, auto_delete=False
    ) -> None:
        with self.sync_connection():
            self._server_conn.add_callback(
                self.__declare_queue,
                queue_name,
                passive=False,
                durable=durable,
                exclusive=exclusive,
                auto_delete=auto_delete,
            )

    def queue_check(self, queue_name: str) -> bool:
        self._server_conn.wait_for_reconnect()
        self._server_conn.add_callback(self.__declare_queue, queue_name, passive=True)
        try:
            self._wait_for_processing()
        except Exception:
            return False
        return True

    def __delete_queue(self, queue_name: str, if_unused=False, if_empty=False) -> None:
        with self._sync_caller():
            self._server_conn._channel.queue_delete(
                queue_name, if_unused=if_unused, if_empty=if_empty
            )

    def queue_delete(self, queue_name: str, if_unused=False, if_empty=False) -> None:
        with self.sync_connection():
            self._server_conn.add_callback(
                self.__delete_queue, queue_name, if_unused=if_unused, if_empty=if_empty
            )

    def __delete_exchange(self, exchange_name: str, if_unused=False) -> None:
        with self._sync_caller():
            self._server_conn._channel.exchange_delete(
                exchange_name, if_unused=if_unused
            )

    def exchange_delete(self, exchange_name: str, if_unused=False) -> None:
        with self.sync_connection():
            self._server_conn.add_callback(
                self.__delete_exchange, exchange_name, if_unused=if_unused
            )

    def __bind_queue(
        self, queue_name: str, exchange_name: str, routing_key: Optional[str] = None
    ) -> None:
        with self._sync_caller():
            self._server_conn._channel.queue_bind(
                queue_name, exchange_name, routing_key=routing_key
            )

    def queue_bind(
        self, queue_name: str, exchange_name: str, routing_key: Optional[str] = None
    ) -> None:
        with self.sync_connection():
            self._server_conn.add_callback(
                self.__bind_queue, queue_name, exchange_name, routing_key=routing_key
            )

    def __unbind_queue(
        self,
        queue_name: str,
        exchange_name: str,
        routing_key: Optional[str] = None,
    ) -> None:
        with self._sync_caller():
            self._server_conn._channel.queue_unbind(
                queue_name, exchange_name, routing_key=routing_key
            )

    def queue_unbind(
        self,
        queue_name: str,
        exchange_name: str,
        routing_key: Optional[str] = None,
    ) -> None:
        with self.sync_connection():
            self._server_conn.add_callback(
                self.__unbind_queue,
                queue_name,
                exchange_name,
                routing_key=routing_key,
            )

    def __purge_queue(self, queue_name: str) -> None:
        with self._sync_caller():
            self._server_conn._channel.queue_purge(queue_name)

    def queue_purge(self, queue_name: str) -> None:
        with self.sync_connection():
            self._server_conn.add_callback(self.__purge_queue, queue_name)

    def _wait_for_processing(self, to_raise: Optional[Exception] = None) -> None:
        """waits for the pika connection to finish whatever processing it's doing
            and raises any exceptions that occur in the backend in the caller thread.

        Args:
            to_raise (Optional[Exception], optional):
                Custom exception to raise if exception was detected in pika backend. Defaults to None.

        Raises:
            to_raise: Either the exception that occurred in the backend or the custom exception
        """
        self._processing.wait()
        self._processing.clear()
        if self._processing_error is None:
            return
        err = self._processing_error
        self._processing_error = None
        raise to_raise or err

    def __del__(self) -> None:
        self.close()

    def __enter__(self):
        return self

    def __exit__(self) -> None:
        self.close()

    def close(self) -> None:
        if self._server_conn is not None:
            self._server_conn.close()


class PikaPublisher(PikaClient):
    def __init__(
        self,
        credentials: Optional[MQCredentials] = None,
        server: str = "localhost",
        port: int = 5672,
        exchange="",
    ) -> None:
        super().__init__(credentials, server, port)
        self.__exchange = exchange
        self._publish_props = pika.BasicProperties(
            delivery_mode=1, content_type="application/json"
        )

    @property
    def exchange(self) -> str:
        return self.__exchange

    @exchange.setter
    def exchange(self, new_exchange: str) -> None:
        if not self.check_exchange(new_exchange):
            raise ValueError(
                f"exchange '{new_exchange}' doesn't exist on '{self.server}'!"
            )
        self.__exchange = new_exchange

    def __publish(self, channel: BlockingChannel, message: Tuple[str, str]) -> None:
        with self._sync_caller():
            channel.basic_publish(
                self.__exchange,
                routing_key=message[1],
                body=message[0],
                properties=self._publish_props,
            )

    def publish(self, body: str, routing_key: str = "", confirm_delivery=False) -> None:
        if not confirm_delivery:
            self._server_conn.add_callback(
                self.__publish,
                self._server_conn._channel,
                (body, routing_key),
            )
            return
        with self.sync_connection():
            self._server_conn.add_callback(
                self.__publish,
                self._server_conn._confirmed_channel,
                (body, routing_key),
            )

    def __del__(self) -> None:
        return super().__del__()


class PikaConsumer(PikaClient):
    """Not yet implemented"""
