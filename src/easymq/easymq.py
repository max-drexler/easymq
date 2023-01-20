import atexit
import json
import socket
import threading
from time import sleep
from typing import Collection, Dict, Iterable, Iterator, List, Optional, Tuple, Union

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
)

# Create a publisher class

__all__ = ["MQClient", "AMQP_MSG"]

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


class _ServerPublisher(threading.Thread):
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
        self._recon_atmps = MQClient.NUM_RECONNECTS

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
        self._unconfirm_channel.exchange_declare(new_exchange, **kwargs)
        self._exchange = new_exchange

    def _check_exchange(self, exchange: str) -> None:
        self._recon_event.wait()
        try:
            self._unconfirm_channel.exchange_declare(exchange, passive=True)
            self._exchange = exchange
        except Exception as e:
            self._pika_err = e
        self._ready_event.set()

    def set_exchange(self, new_exchange: str, create=False, **kwargs) -> None:
        if not isinstance(new_exchange, str):
            raise TypeError("exchange must be a string")

        if create:
            return self._connection.add_callback_threadsafe(
                lambda: self._create_exchange(new_exchange, **kwargs)
            )

        self._connection.add_callback_threadsafe(
            lambda: self._check_exchange(new_exchange)
        )
        self._ready_event.wait()
        self._ready_event.clear()
        if self._pika_err is not None:
            self._pika_err = None
            raise ValueError(
                f"exchange '{new_exchange}' doesn't exist, use the kwarg create to create it on the server"
            )

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

    def _reconnect(self) -> None:
        self._recon_event.clear()
        self._connection.sleep(MQClient.RECONNECT_DELAY)

        if self._recon_atmps is not None:
            self._recon_atmps -= 1
            if self._recon_atmps < 0:
                self.stop()
                raise RuntimeWarning(f"Could not reconnect to {self.server} after {MQClient.NUM_RECONNECTS} attempt(s), exiting")
        try:
            self._connect()
        except ConnectionError:
            self._reconnect()

        self._recon_atmps = MQClient.NUM_RECONNECTS
        self._recon_event.set()

    def _publish(self, message: Tuple[str, str], confirm_delivery: bool) -> None:
        self._recon_event.wait()

        channel: BlockingChannel = (
            self._confirmed_channel if confirm_delivery else self._unconfirm_channel
        )

        try:
            channel.basic_publish(self._exchange, message[0], message[1])
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

        self._ready_event.wait()
        self._ready_event.clear()
        if self._pika_err is not None:
            err = self._pika_err
            self._pika_err = None
            raise err

    def stop(self) -> None:
        self._is_running = False
        self._connection.process_data_events(time_limit=1)
        if self._connection.is_open:
            self._connection.close()

    def __str__(self) -> str:
        return f"{self._params.host}: {self._exchange}"

    def __eq__(self, other) -> bool:
        if isinstance(other, str):
            return other == self.server
        if not isinstance(other, _ServerPublisher):
            return False
        other: _ServerPublisher

        return other.server == self.server and other.exchange == self.exchange

# probably rename to Publisher and PublisherPool

class MQClient:

    NUM_RECONNECTS = 0
    RECONNECT_DELAY = 15

    def __init__(
        self,
        username: str = "guest",
        password: str = "guest",
        auth_file: Optional[str] = None,
        exchange = ''
    ) -> None:
        # make sure that there are no run-away threads when exiting
        atexit.register(self.close)
        self._exchange = exchange

        if auth_file is not None:
            with open(auth_file, "r", encoding="utf-8") as creds:
                username: str = creds.readline().strip()
                # change this so password isn't stored in memory??
                pswd: str = creds.readline().strip()
        else:
            username = username
            __pswd = password

        self._credentials = pika.PlainCredentials(
            username, __pswd, erase_on_connect=True
        )

        try:
            self._connections: List[_ServerPublisher] = [
                _ServerPublisher(
                    pika.ConnectionParameters(
                        host="localhost", credentials=self._credentials
                    )
                )
            ]
        except Exception:
            self._connections: List[_ServerPublisher] = []
            raise RuntimeWarning("Couldn't connect to localhost with given credentials")

    def set_exchange(self, new_exchange: str) -> None:
        pass
    
    @property
    def exchange(self) -> Union[str, None]:
        return self._exchange
    
    @property
    def connections(self) -> List[str]:
        return [serv.server for serv in self._connections]

    def _pickle_amqp_msg(key, payload):
        return (str(key), json.dumps(payload))

    # if connect() isn't called we should publish to localhost
    def publish(self, message: Union[AMQP_MSG, Iterable[AMQP_MSG]]) -> None:
        if not self._connections:
            raise RuntimeError("Not connected to a server, call the connect() function")

        messages = [message] if isinstance(message, tuple) else message
        for msg in messages:
            self.__pub_con.send(MQClient._pickle_amqp_msg(*msg))

    def connect(self, new_servers: Union[str, Iterable[str]]):
        servers = [new_servers] if isinstance(new_servers, str) else new_servers

        new_connects: List[_ServerPublisher] = []
        for server in servers:
            publisher = None
            try:
                publisher = self._connections.pop(self._connections.index(server))
            except ValueError:   
                publisher = _ServerPublisher(
                    pika.ConnectionParameters(host=server, credentials=self._credentials),
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

    def __getitem__(self, key) -> _ServerPublisher:
        if not isinstance(key, str):
            raise ValueError("index a MQClient with server names")
        found = next((pub for pub in self._connections if pub.server == key), None)
        if found is None:
            raise AttributeError(f"{key} is not in this client")
        return found
        
    def __iter___(self) -> Iterator:
        return iter(self._connections)

    def __contains__(self, item) -> bool:
        if isinstance(item, str):
            return item in [pub.server for pub in self._connections]
        if isinstance(item, _ServerPublisher):
            return item in self._connections
        return False
