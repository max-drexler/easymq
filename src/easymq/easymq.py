import json
import multiprocessing
from multiprocessing.synchronize import Event as EventClass
from multiprocessing.connection import Connection as ConnectionClass
from queue import Empty
from time import sleep
import socket
import atexit
import pika
import pika.channel
from pika.exceptions import ProbableAccessDeniedError, ProbableAuthenticationError, AuthenticationError, ConnectionWrongStateError
from typing import (List,
                    Tuple,
                    Dict,
                    Union,
                    Collection,
                    Iterable,
                    Optional)

__all__ = ['MQClient', 'AMQP_MSG']

JSONType = Union[str, int, float, bool, None, Dict[str, Union[str,
                                                              bool, int, float, dict]], List[Union[str, bool, int, float, dict]]]
AMQP_MSG = Tuple[str, JSONType]


class MQClient:

    # what to do if a message fails to deliver:
    # 0: raise exception
    # 1: ignore
    # 2: 
    MSG_FAIL_POLICY = 0
    NUM_RECONNECTS  = 3
    RECONNECT_DELAY = 15

    _pickle_amqp_msg = lambda key, payload: (str(key), json.dumps(payload))

    def __init__(self,
                 username: str = 'guest',
                 password: str = 'guest',
                 auth_file: Optional[str] = None
                 ) -> None:
        if auth_file is not None:
            with open(auth_file, 'r', encoding='utf-8') as creds:
                self.username: str = creds.readline().strip()
                self.__pswd: str = creds.readline().strip()  # change this so password isn't stored in memory??
        else:
            self.username = username
            self.__pswd = password

        self.__connections: List[ServerConnection] = []
        self.__closed = False

        # verify that client_con won't be garbage collected w/o local reference
        self.__pub_con, client_con: ConnectionClass  = multiprocessing.Pipe()
        self.__stop_publish: EventClass = multiprocessing.Event()
        self.__publisher = ExchangePublisher(client_con, self.__stop_publish)
        self.__publisher.start()

        # make sure that there are no run-away processes when exiting
        atexit.register(self.close)

    @property
    def exchange(self) -> Union[str, None]:
        pub_exchange = self.__publisher.exchange
        return pub_exchange if pub_exchange else None

    @exchange.setter
    def exchange(self, new_exchange: str) -> None:
        self.__publisher.exchange = new_exchange

    @property
    def connections(self) -> List[str]:
        return [serv.server for serv in self.__connections]
    
    @property
    def open_connections(self) -> List[str]:
        return [serv.server for serv in self.__connections if serv.is_open]
        
    def send(self, message: Union[AMQP_MSG, Iterable[AMQP_MSG]]) -> None:
        if not self.__connections:
            raise RuntimeError("Not connected to a server, call the connect() function")
        if self.__closed:
            raise RuntimeError("Cannot send messages after closing the client")
        
        if self.__pub_con.poll(0.001):
            unsent_msg = self.__pub_con.recv()
            # policy decides what to do with unsent messages
        
        messages = [message] if isinstance(message, tuple) else message
        for msg in messages:
            self.__pub_con.send(MQClient._pickle_amqp_msg(*msg))
        
    def connect(self, servers: Union[str, Iterable[str]]):
        if isinstance(servers, str):
            self.__add_server(servers)
            return self
        map(self.__add_server, servers)
        return self

    def __add_server(self, server: str) -> None:
        recon_atmpts = MQClient.NUM_RECONNECTS
        new_connection = None
        while new_connection is None and (recon_atmpts is None or recon_atmpts > 0):
            try:
                new_connection = ServerConnection(server, self.username, self.__pswd)
            except ConnectionError:
                if recon_atmpts is not None:
                    recon_atmpts -= 1
                sleep(MQClient.RECONNECT_DELAY)
        if new_connection is None:
            raise ConnectionError(f"Coudln't connect to '{server}' after '{MQClient.NUM_RECONNECTS}' retri(es)")
        
        self.__connections.append(new_connection)
        self.__publisher.add_connection(new_connection)

    def close(self) -> None:
        self.__closed = True
        self.__stop_publish.set()
        self.__publisher.join()
        map(lambda con: con.close(), self.__connections)

    def __enter__(self):
        return self

    def __exit__(self, ex_type, ex_value, traceback) -> bool:
        self.close()


class ServerConnection:

    def __init__(self, server, username, password) -> None:
        self.__server = server

        creds = pika.PlainCredentials(username, password)
        self.__connection_params = pika.ConnectionParameters(
            credentials=creds, host=self.__server)

        try:
            self.__connection = pika.BlockingConnection(
                parameters=self.__connection_params)
        except socket.gaierror:
            raise ConnectionError(f"Could not Connect to server '{self.__server}'")
        except (ProbableAccessDeniedError, ProbableAuthenticationError, AuthenticationError):
            raise AuthenticationError(
                f"User '{username}' not authorized to connect to '{self.__server}'")
        self.__channel = self.__connection.channel()

    @property
    def channel(self) -> pika.channel.Channel:
        return self.__channel

    @property
    def server(self) -> str:
        return self.__server

    @property
    def connection(self) -> pika.BlockingConnection:
        return self.__connection

    @property
    def is_open(self) -> bool:
        return self.__channel.is_open

    def close(self) -> None:
        if self.__channel.is_open:
            self.__channel.close()
        if self.__connection.is_open:
            self.__connection.close()

    def reconnect(self) -> bool:
        if self.__connection.is_open:
            if not self.__channel.is_open:
                self.__channel = self.__connection.channel()
            return True
        try:
            self.__connection = pika.BlockingConnection(
                parameters=self.__connection_params
            )
            self.__channel = self.__connection.channel()
        except Exception:
            return False
        return True

# what to do with messages that can't be sent/timeout??
class ExchangePublisher(multiprocessing.Process):

    def __init__(self, client_connection: ConnectionClass, stop_sig: EventClass, connections: List[ServerConnection] = [], exchange: Optional[str] = None) -> None:
        super().__init__(None, None, 'EasyMQ-Publisher', (), {})
        self.__cl_conn = client_connection
        self.connections = connections
        self.__stop: EventClass = stop_sig
        self.__exchange = exchange or ''

        self._pub_props = pika.BasicProperties(content_type='text/json', delivery_mode=1)

    @property
    def exchange(self) -> str:
        return self.__exchange
    
    @exchange.setter
    def exchange(self, new_exchange: str) -> None:
        if not isinstance(new_exchange, str):
            raise TypeError('New exchange must be a string')
        self.__exchange = new_exchange
        
    def run(self) -> None:
        # 1. get new message
        # 3. try to publish to all servers
        # 4. retry on all servers that aren't open
        # will probably have to turn this class asychronous
        while True:
            if self.__stop.is_set():
                break

            if not self.__cl_conn.poll(timeout=0.1):
                continue
            
            key, payload: str = self.__cl_conn.recv()

            for conn in self.connections:
                conn.channel.basic_publish(self.__exchange, key, payload, self._pub_props)
                # create timer that calls __republish
        # write unsent messages to file

    def __republish(connection, message, atmpt_num) -> None:
        pass

    def add_connection(self, new_connection: ServerConnection) -> None:
        self.connections.append(new_connection)
