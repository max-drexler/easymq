import atexit
import json
from typing import Any, Dict, Iterable, Iterator, List, Optional, Union, Tuple
import warnings

from .config import DEFAULT_EXCHANGE, DEFAULT_SERVER, DEFAULT_ROUTE_KEY
from .adapter import MQCredentials, PikaPublisher

__all__ = ["PublisherPool", "Publisher"]

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


class Publisher:
    def __init__(self) -> None:
        atexit.register(self.close)

        self._credentials = MQCredentials().creds
        self._publisher: PikaPublisher = None
        try:
            self._publisher = PikaPublisher(
                credentials=self._credentials,
                server=DEFAULT_SERVER,
                exchange=DEFAULT_EXCHANGE,
            )
        except Exception:
            warnings.warn(
                f"User {self._credentials.username} cannot connect to {DEFAULT_SERVER} with given credentials"
            )

    @property
    def exchange(self) -> Union[str, None]:
        if self._publisher is None:
            return None
        return self._publisher.exchange

    @exchange.setter
    def exchange(self, new_exchange: str) -> None:
        if self._publisher is None:
            raise ConnectionError("Publisher isn't connected to a server!")
        self._publisher.exchange = new_exchange

    @property
    def server(self) -> Union[str, None]:
        if self._publisher is None:
            return None
        return self._publisher.server

    def exchange_exists(self, new_exchange: str) -> bool:
        return self._publisher.check_exchange(new_exchange)

    def publish(self, payload: JSONType, route_key=DEFAULT_ROUTE_KEY, confirm_delivery=False) -> None:
        if self._publisher is None:
            raise ConnectionError("Publisher isn't connected to a server!")
        self._publisher.publish(
            json.dumps(payload),
            routing_key=str(route_key),
            confirm_delivery=confirm_delivery,
        )

    def publish_all(
        self, messages: Iterable[Union[JSONType, AMQP_MSG]], confirm_delivery=False
    ) -> None:
        for msg in messages:
            if isinstance(msg, tuple):
                self.publish(
                    msg[1],
                    route_key=msg[0],
                    confirm_delivery=confirm_delivery,
                )
            else:
                self.publish(msg, confirm_delivery=confirm_delivery)

    def connect(self, server: str, credentials: Optional[MQCredentials] = None):
        # don't want to connect if server and credentials aren't different
        if (
            self._publisher is not None
            and self._publisher.server == server
            and (credentials is None or self._credentials == credentials)
        ):
            return

        new_pub = PikaPublisher(
            credentials=credentials or self._credentials, server=server
        )
        try:
            new_pub.exchange = self._publisher.exchange
        except Exception:
            warnings.warn(
                f"Exchange '{self._publisher.exchange}' doesn't exist on {server}, connected to default exchange"
            )
        self.close()
        self._publisher = new_pub
        if credentials is not None:
            self._credentials = credentials
        return self

    def close(self) -> None:
        if self._publisher is not None:
            self._publisher.close()
            self._publisher = None

    def __enter__(self):
        return self

    def __exit__(self) -> None:
        self.close()

    def __del__(self) -> None:
        self.close()
        self._credentials = None

    def __eq__(self, __o: object) -> bool:
        if isinstance(__o, str):
            return __o == self.server
        else:
            return id(__o) == id(self)

    def __hash__(self) -> int:
        return id(self)

    def __call__(self, func_name: str, *args: Any, **kwds: Any) -> Any:
        client_function = getattr(self._publisher, func_name, None)
        if client_function is None:
            raise AttributeError(f"Function {func_name} doesn't exist")

        client_function(*args, **kwds)


class PublisherPool:
    def __init__(
        self,
        credentials: Optional[MQCredentials] = None,
        exchange="",
    ) -> None:
        # make sure that there are no run-away threads when exiting
        atexit.register(self.close)
        self._credentials = getattr(credentials, "creds", MQCredentials().creds)

        try:
            self._publishers: List[Publisher] = [
                Publisher(
                    credentials=self._credentials,
                    exchange=exchange,
                )
            ]
        except Exception:
            self._publishers: List[Publisher] = []
            warnings.warn("Couldn't connect to localhost with given credentials")

    def set_exchange(self, new_exchange: str) -> None:
        for pub in self._publishers:
            try:
                pub.exchange = new_exchange
            except Exception:
                warnings.warn(
                    f"Couldn't set exchange on {pub.server}! Keeping exchange as {pub.exchange}"
                )

    def get_exchanges(self) -> Union[str, List[str], None]:
        if not self._publishers:
            return None
        exchanges: List[str] = []
        for pub in self._publishers:
            if pub.exchange not in exchanges:
                exchanges.append(pub.exchange)
        if len(exchanges) == 1:
            return exchanges[0]
        return exchanges

    @property
    def connections(self) -> List[str]:
        return [serv.server for serv in self._publishers]

    def publish(self, payload: JSONType, route_key="", confirm_delivery=False) -> None:
        if not self._publishers:
            warnings.warn("Not connected to a server, call connect()")
            return

        for pub in self._publishers:
            pub.publish(payload, route_key=route_key, confirm_delivery=confirm_delivery)

    def publish_all(
        self, messages: Iterable[Union[JSONType, AMQP_MSG]], confirm_delivery=False
    ) -> None:
        """publish multiple messages to connected server(s)

        Args:
            messages (Iterable[Union[AMQP_MSG, JSONType]]): messages to publish.
                Messages should either contain tuples of the following form (routing key, payload) or
                just payloads, [payload, payload, ...], to use the default routing key: '#'.
        """
        if not self._publishers:
            warnings.warn("Not connected to a server, call the connect() function")
            return

        for pub in self._publishers:
            pub.publish_all(messages, confirm_delivery=confirm_delivery)

    def connect(self, server_list: Union[str, Iterable[str]], exchange=""):
        servers = [server_list] if isinstance(server_list, str) else server_list
        new_pubs: List[Publisher] = []

        for pub in self._publishers:
            if pub.server in servers:
                new_pubs.append(self._publishers.remove(pub))
        return self

    def close(self) -> None:
        for con in self._publishers:
            con.close()
        self._publishers = []

    def __enter__(self):
        return self

    def __exit__(self, ex_type, ex_value, traceback) -> bool:
        self.close()

    def __str__(self) -> str:
        return f"MQClient publishing to {', '.join(self._publishers)}"

    def __len__(self) -> int:
        return len(self._publishers)

    def __delitem__(self, key) -> None:
        try:
            pub = self.__getitem__(key)
        except AttributeError:
            return
        pub.stop()
        self._publishers.remove(pub)

    def __getitem__(self, key) -> Publisher:
        if isinstance(key, int):
            return self._publishers[key]
        if not isinstance(key, str):
            raise ValueError("index a MQClient with a server name or integer")
        found = next((pub for pub in self._publishers if pub.server == key), None)
        if found is None:
            raise AttributeError(f"{key} is not in this client")
        return found

    def __iter___(self) -> Iterator:
        return iter(self._publishers)

    def __contains__(self, item) -> bool:
        if isinstance(item, str):
            return item in [pub.server for pub in self._publishers]
        if isinstance(item, Publisher):
            return item in self._publishers
        return False

    def __del__(self) -> None:
        self.close()
        self._publishers = None
        self._credentials = None
