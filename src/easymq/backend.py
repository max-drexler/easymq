import socket
import threading
import time
import atexit
from typing import Optional, Tuple, Callable

import pika
from pika.adapters.blocking_connection import BlockingChannel
from pika.exceptions import (
    AuthenticationError,
    ChannelClosedByBroker,
    ConnectionClosed,
    ConnectionWrongStateError,
    NackError,
    ProbableAccessDeniedError,
    ProbableAuthenticationError,
    UnroutableError,
    AMQPConnectionError,
)

from easymq.config import RECONNECT_TRIES, RECONNECT_DELAY


class MQCredentials:
    def __init__(
        self, username="guest", password="guest", auth_file: Optional[str] = None
    ) -> None:
        if auth_file is not None:
            with open(auth_file, "r", encoding="utf-8") as creds:
                username: str = creds.readline().strip()
                __pswd: str = creds.readline().strip()
        else:
            username = username
            __pswd = password

        self._credentials = pika.PlainCredentials(username, __pswd)

    @property
    def creds(self) -> pika.PlainCredentials:
        return self._credentials


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

        self.connect()
        self.start()

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
            raise ConnectionError(f"Could not connect to server")
        except (
            AuthenticationError,
            ProbableAccessDeniedError,
            ProbableAuthenticationError,
        ):
            raise AuthenticationError(f"Not authenticated to connect to server")

    def close(self) -> None:
        self._running = False
        if self._connection is None or self._connection.is_closed:
            return
        self._connection.process_data_events(time_limit=1)
        if self._connection.is_open:
            self._connection.close()

    def wait_for_reconnect(self, timeout=None) -> bool:
        self._reconnecting.wait(timeout=timeout)
        return self.is_reconnecting

    def __channel_setup(self) -> None:
        if self._channel is None or self._channel.is_closed:
            self._channel = self._connection.channel()

    def __reconnect(self) -> None:
        self._reconnecting.clear()
        tries = RECONNECT_TRIES
        while True:
            if tries is not None:
                tries -= 1
                if tries < 0:
                    self.close()
                    self._reconnecting.set()
                    raise RuntimeWarning(
                        f"Could not reconnect to {self.server} after {RECONNECT_TRIES} attempt(s), exiting..."
                    )
            try:
                self.connect()
                if self._connection.is_open:
                    break
            except AMQPConnectionError:
                time.sleep(RECONNECT_DELAY)
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
    ) -> None:
        self._credentials: pika.PlainCredentials = (
            getattr(credentials, "creds", None) or MQCredentials().creds
        )
        self._server_conn = PikaConnection(
            pika.ConnectionParameters(host=server, credentials=self._credentials)
        )
        atexit.register(self._server_conn.close)
        
        self._processing = threading.Event()
        self._processing_error = None

    def _create_exchange(self, new_exchange: str, **kwargs) -> None:
        self._server_conn.wait_for_reconnect()
        try:
            self._unconfirm_channel.exchange_declare(new_exchange, **kwargs)
            self._exchange = new_exchange
        except ChannelClosedByBroker as e:
            self._pika_err = e
            self._setup_channels()
        self._ready_event.set()

    def _check_exchange(self, exchange: str) -> None:
        self._recon_event.wait()
        try:
            self._unconfirm_channel.exchange_declare(exchange, passive=True)
        except ChannelClosedByBroker as e:
            self._pika_err = e
            self._setup_channels()
        except Exception as e:
            self._pika_err = e
        self._ready_event.set()

    def __declare_exchange(
        self,
        exchange_name: str,
        exchange_type: str = "direct",
        durable=False,
        auto_delete=False,
        internal=False,
    ) -> None:
        self._server_conn.wait_for_reconnect()
        try:
            self._server_conn._channel.exchange_declare(
            exchange_name,
            exchange_type=exchange_type,
            durable=durable,
            auto_delete=auto_delete,
            internal=internal,
            )
        except Exception as e:
            self._processing_error = e
        self._processing.set()

    def declare_exchange(
        self,
        exchange_name: str,
        exchange_type: str = "direct",
        durable=False,
        auto_delete=False,
        internal=False,
    ) -> None:
        self._server_conn.wait_for_reconnect()

        self._server_conn.add_callback(
            self.__declare_exchange,
            exchange_name,
            exchange_type=exchange_type,
            durable=durable,
            auto_delete=auto_delete,
            internal=internal,
        )

        self._wait_for_processing()

    def check_exchange(self, new_exchange: str) -> bool:
        if not isinstance(new_exchange, str):
            raise TypeError("exchange must be a string")
        self._connection.add_callback_threadsafe(
            lambda: self._check_exchange(new_exchange)
        )
        try:
            self._wait_for_processing()
        except Exception:
            return False
        return True

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
        self._server_conn.close()

class PikaPublisher(PikaClient):
    def __init__(
        self, connection_params: pika.ConnectionParameters, exchange=""
    ) -> None:
        super().__init__(connection_params, exchange)
        self._pub_params = pika.BasicProperties(
            content_type="text/json", delivery_mode=1
        )

    def _publish(self, message: Tuple[str, str], confirm_delivery: bool) -> None:
        self._recon_event.wait()

        channel: BlockingChannel = (
            self._confirmed_channel if confirm_delivery else self._unconfirm_channel
        )

        try:
            channel.basic_publish(
                self._exchange,
                routing_key=message[0],
                body=message[1],
                properties=self._pub_params,
            )
        except (UnroutableError, NackError) as e:
            self._pika_err = e
        except ChannelClosedByBroker as e:
            self._pika_err = e
            self._setup_channels()
        except Exception as e:
            self._pika_err = e
            self._connect()

        if confirm_delivery:
            self._ready_event.set()

    def publish(self, message: Tuple[str, str], confirm_delivery=False) -> None:
        if not self._is_running:
            raise RuntimeWarning("Publisher not running, can't publish messages!")

        self._connection.add_callback_threadsafe(
            lambda: self._publish(message, confirm_delivery)
        )

        if not confirm_delivery:
            return

        self.wait_and_check()

    def set_exchange(self, new_exchange: str, create=False, **kwargs) -> None:
        if not isinstance(new_exchange, str):
            raise TypeError("exchange must be a string")

        if not create:
            self._connection.add_callback_threadsafe(
                lambda: self._check_exchange(new_exchange)
            )
            self._wait_for_processing(
                ValueError(f"exchange '{new_exchange}' doesn't exist")
            )
        self._connection.add_callback_threadsafe(
            lambda: self._create_exchange(new_exchange, **kwargs)
        )
        self._wait_for_processing()
