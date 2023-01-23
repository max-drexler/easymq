import atexit
import json
import socket
import threading
from typing import Collection, Dict, Iterable, Iterator, List, Optional, Tuple, Union
import warnings

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
    AMQPConnectionError
)

__all__ = ["PublisherPool", "Publisher", "MQCredentials", "JSONType", "AMQP_MSG"]

JSONType = Union[
    str,
    int,
    float,
    bool,
    None,
    Dict[str, Union[str, bool, int, float, dict]],
    List[Union[str, bool, int, float, dict]],
]
AMQP_MSG = Tuple[str, JSONType]


class _PikaClient(threading.Thread):
    def __init__(
        self,
        con_params: pika.ConnectionParameters,
        exchange="",
        auto_run=True,
    ) -> None:
        super().__init__(None, None, "name", (), {}, daemon=False)
        self._params = con_params
        self._exchange = exchange

        self._is_running = False
        self._recon_event = threading.Event()
        self._recon_event.set()
        self._recon_atmps = PublisherPool.NUM_RECONNECTS
        self._pub_params = pika.BasicProperties(
            content_type="text/json", delivery_mode=1
        )
        self._connection = None
        self._unconfirm_channel = None
        self._confirmed_channel = None
        self._ready_event = threading.Event()
        self._pika_err = None

        self._connect()

        if auto_run:
            self.start()

        if self._exchange != "":
            pass

    def _setup_connection(self) -> None:
        if self._connection is not None and self._connection.is_open:
            return
        try:
            self._connection = pika.BlockingConnection(parameters=self._params)
        except (socket.gaierror, socket.herror):
            raise ConnectionError(f"Could not connect to server")
        except (
            AuthenticationError,
            ProbableAccessDeniedError,
            ProbableAuthenticationError,
        ):
            raise AuthenticationError(f"Not authenticated to connect to server")

    def _setup_channels(self) -> None:
        self._setup_connection()
        if self._unconfirm_channel is None or self._unconfirm_channel.is_closed:
            self._unconfirm_channel = self._connection.channel()
        if self._confirmed_channel is None or self._confirmed_channel.is_closed:
            self._confirmed_channel = self._connection.channel()
            self._confirmed_channel.confirm_delivery()

    def _connect(self) -> None:
        self._setup_connection()
        self._setup_channels()

    def _create_exchange(self, new_exchange: str, **kwargs) -> None:
        self._recon_event.wait()
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

    def check_exchange(self, new_exchange: str) -> bool:
        if not isinstance(new_exchange, str):
            raise TypeError("exchange must be a string")
        self._connection.add_callback_threadsafe(
            lambda: self._check_exchange(new_exchange)
        )
        try:
            self.wait_and_check()
        except Exception:
            return False
        return True

    def set_exchange(self, new_exchange: str, create=False, **kwargs) -> None:
        if not isinstance(new_exchange, str):
            raise TypeError("exchange must be a string")

        if not create:
            self._connection.add_callback_threadsafe(
                lambda: self._check_exchange(new_exchange)
            )
            self.wait_and_check(ValueError(f"exchange '{new_exchange}' doesn't exist"))
        self._connection.add_callback_threadsafe(
            lambda: self._create_exchange(new_exchange, **kwargs)
        )
        self.wait_and_check()

    def wait_and_check(self, to_raise: Optional[Exception] = None) -> None:
        """waits for the pika backend to finish whatever processing it's doing
            and raises any exceptions that occur in the backend in the caller thread.

        Args:
            to_raise (Optional[Exception], optional): 
                Custom exception to raise if exception was detected in pika backend. Defaults to None.

        Raises:
            to_raise: Either the exception that occurred in the backend or the custom exception
        """
        self._ready_event.wait()
        self._ready_event.clear()
        if self._pika_err is None:
            return
        err = self._pika_err
        self._pika_err = None
        raise to_raise or err

    @property
    def exchange(self) -> str:
        return self._exchange

    @property
    def server(self) -> str:
        return self._params.host

    def run(self) -> None:
        self._is_running = True
        while self._is_running:
            try:
                self._connection.process_data_events(time_limit=1)
            except ConnectionClosed:
                self._reconnect()
            except AMQPConnectionError:
                self._reconnect()

    def _reconnect(self) -> None:
        self._recon_event.clear()
        self._connection.sleep(PublisherPool.RECONNECT_DELAY)

        if self._recon_atmps is not None:
            self._recon_atmps -= 1
            if self._recon_atmps < 0:
                self.stop()
                raise RuntimeWarning(
                    f"Could not reconnect to {self.server} after {PublisherPool.NUM_RECONNECTS} attempt(s), exiting"
                )
        try:
            self._connect()
        except ConnectionError:
            self._reconnect()

        self._recon_atmps = PublisherPool.NUM_RECONNECTS
        self._recon_event.set()

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

    def stop(self) -> None:
        self._is_running = False
        if self._connection.is_closed:
            return
        self._connection.process_data_events(time_limit=1)
        if self._connection.is_open:
            self._connection.close()

    def __str__(self) -> str:
        return f"{self._params.host}: {self._exchange}"

    def __hash__(self) -> int:
        return id(self)

    def __eq__(self, other) -> bool:
        if isinstance(other, str):
            return other == self.server
        if not isinstance(other, _PikaClient):
            return False
        other: _PikaClient
        return other.server == self.server and other.exchange == self.exchange


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


class Publisher:
    def __init__(
        self,
        credentials: Optional[MQCredentials] = None,
        exchange: str = "",
        server="localhost",
    ) -> None:
        atexit.register(self.close)

        self._credentials = getattr(credentials, "creds", None) or MQCredentials().creds
        self._publisher: _PikaClient = None
        try:
            self._publisher = _PikaClient(
                pika.ConnectionParameters(host=server, credentials=self._credentials),
                exchange=exchange,
            )
        except Exception:
            warnings.warn(
                f"User {self._credentials.username} cannot connect to {server} with given credentials"
            )

    @property
    def exchange(self) -> str:
        if self._publisher is None:
            return ""
        return self._publisher.exchange

    @property
    def server(self) -> Union[str, None]:
        if self._publisher is None:
            return None
        return self._publisher.server

    def check_exchange(self, new_exchange: str) -> bool:
        return self._publisher.check_exchange(new_exchange)

    def set_exchange(self, new_exchange: str) -> None:
        self._publisher.set_exchange(new_exchange, create=False)

    def create_exchange(self, new_exchange: str, **kwargs) -> None:
        self._publisher.set_exchange(new_exchange, create=True, **kwargs)

    def publish(self, payload: JSONType, route_key="#", confirm_delivery=False) -> None:
        self._publisher.publish(
            (route_key, json.dumps(payload)), confirm_delivery=confirm_delivery
        )

    def publish_all(
        self, messages: Iterable[Union[JSONType, AMQP_MSG]], confirm_delivery=False
    ) -> None:
        for msg in messages:
            if isinstance(msg, tuple):
                self.publish(
                    json.dumps(msg[1]),
                    route_key=str(msg[0]),
                    confirm_delivery=confirm_delivery,
                )
            else:
                self.publish(json.dumps(msg), confirm_delivery=confirm_delivery)

    def connect(self, server: str) -> None:
        if server == getattr(self._publisher, "server", None):
            return
        new_publisher = _PikaClient(
            pika.ConnectionParameters(host=server, credentials=self._credentials),
            exchange=self.exchange,
        )
        self.close()
        self._publisher = new_publisher

    def close(self) -> None:
        if self._publisher is not None:
            self._publisher.stop()

    def __enter__(self):
        return self

    def __exit__(self) -> None:
        self.close()


class PublisherPool:

    NUM_RECONNECTS = 0
    RECONNECT_DELAY = 15

    def __init__(
        self,
        credentials: Optional[MQCredentials] = None,
        exchange="",
    ) -> None:
        # make sure that there are no run-away threads when exiting
        atexit.register(self.close)
        self._exchange = exchange
        self._credentials = getattr(credentials, "creds", None) or MQCredentials().creds

        try:
            self._connections: List[Publisher] = [
                Publisher(
                    credentials=self._credentials,
                    exchange=self._exchange,
                )
            ]
        except Exception:
            self._connections: List[Publisher] = []
            warnings.warn("Couldn't connect to localhost with given credentials")

    def set_exchange(self, new_exchange: str) -> None:
        for pub in self._connections:
            try:
                pub.set_exchange(new_exchange)
            except Exception:
                warnings.warn(
                    f"Couldn't set exchange on {pub.server}! (Exchange doesn't exist)"
                )

    def create_exchange(self, new_exchange: str, **kwargs) -> None:
        for pub in self._connections:
            pub.set_exchange(new_exchange, create=True, **kwargs)

    @property
    def exchange(self) -> Union[str, None]:
        return self._exchange

    @property
    def connections(self) -> List[str]:
        return [serv.server for serv in self._connections]

    def publish(self, payload: str, route_key="#", confirm_delivery=False) -> None:
        if not self._connections:
            warnings.warn("Not connected to a server, call connect()")
            return

        for pub in self._connections:
            pub.publish((payload, route_key), confirm_delivery=confirm_delivery)

    def publish_all(self, messages: Iterable[Union[AMQP_MSG, JSONType]]) -> None:
        """publish multiple messages to connected server(s)

        Args:
            messages (Iterable[Union[AMQP_MSG, JSONType]]): messages to publish.
                Messages should either contain tuples of the following form (routing key, payload) or
                just payloads, [payload, payload, ...], to use the default routing key: '#'.
        """
        if not self._connections:
            warnings.warn("Not connected to a server, call the connect() function")
            return

        for msg in messages:
            if isinstance(msg, tuple):
                self.publish(json.dumps(msg[1]), route_key=str(msg[0]))
            else:
                self.publish(json.dumps(msg))

    def connect(self, new_servers: Union[str, Iterable[str]]):
        servers = [new_servers] if isinstance(new_servers, str) else new_servers

        new_connects: List[_PikaClient] = []
        for server in servers:
            publisher = None
            try:
                publisher = self._connections.pop(self._connections.index(server))
            except ValueError:
                publisher = _PikaClient(
                    pika.ConnectionParameters(
                        host=server, credentials=self._credentials
                    ),
                    exchange=self.exchange,
                )
            new_connects.append(publisher)
        self.close()
        self._connections = new_connects

    def close(self) -> None:
        for con in self._connections:
            con.stop()
        self._connections = []

    def __enter__(self):
        return self

    def __exit__(self, ex_type, ex_value, traceback) -> bool:
        self.close()

    def __str__(self) -> str:
        return f"MQClient publishing to {', '.join(self._connections)}"

    def __len__(self) -> int:
        return len(self._connections)

    def __delitem__(self, key) -> None:
        try:
            pub = self.__getitem__(key)
        except AttributeError:
            return
        pub.stop()
        self._connections.remove(pub)

    def __getitem__(self, key) -> _PikaClient:
        if isinstance(key, int):
            return self._connections[key]
        if not isinstance(key, str):
            raise ValueError("index a MQClient with a server name or integer")
        found = next((pub for pub in self._connections if pub.server == key), None)
        if found is None:
            raise AttributeError(f"{key} is not in this client")
        return found

    def __iter___(self) -> Iterator:
        return iter(self._connections)

    def __contains__(self, item) -> bool:
        if isinstance(item, str):
            return item in [pub.server for pub in self._connections]
        if isinstance(item, _PikaClient):
            return item in self._connections
        return False
