import json
import multiprocessing
from multiprocessing.synchronize import Event as EventClass
from queue import Empty
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

__all__ = ['MQClient']

AMQP_MSG = Tuple[str, Dict[str, Union[str, bool, int, float]]]

class MQClient:

    NUM_RECONNECTS = 3
    RECONNECT_DELAY = 10

    def __init__(self, exchange: str, servers: Union[str, Iterable[str]], username: str = 'guest', paswd: str = 'guest', auth_file: Optional[str] = None) -> None:
        self.__exchange: str = exchange
        self.__servers: List[str] = [servers] if isinstance(servers, str) else [serv for serv in servers]

        if auth_file is not None:
            creds = open(auth_file, 'r', encoding='utf-8')
        self.username: str = getattr(creds, 'readline', username).strip()
        self.__pswd: str = getattr(creds, 'readline', paswd).strip()  # change this so password isn't stored in memory??
        if auth_file is not None:
            creds.close()

        self.__msg_queue: multiprocessing.Queue = multiprocessing.Queue()
        self.__stop_sig: EventClass = multiprocessing.Event()

        self.__connections = [ServerConnection(
            self.__exchange, serv, self.username, self.__pswd) for serv in self.__servers]
        self.__publisher = Publisher(
            self.__msg_queue, self.__connections, self.__stop_sig)
        self.__publisher.start()
        atexit.register(self.close)
        self.__open = True

    @property
    def exchange(self) -> str:
        return self.__exchange

    @property
    def servers(self) -> Collection[str]:
        return self.__servers

    def send(self, message: Union[AMQP_MSG, Iterable[AMQP_MSG]]) -> None:
        if not self.__open:
            return  # possibly want to raise error here
        if isinstance(message, tuple):
            self.__msg_queue.put(message)
        else:
            map(self.__msg_queue.put, message)

    def add_server(self, server: str) -> None:
        new_connection = ServerConnection(
            self.__exchange, server, self.username, self.__pswd)
        self.__connections.append(new_connection)
        self.__servers.append(server)
        self.__publisher.add_connection(new_connection)

    def close(self) -> None:
        self.__open = False
        self.__stop_sig.set()
        self.__publisher.join()


class ServerConnection:

    def __init__(self, exchange, server, username, password) -> None:
        creds = pika.PlainCredentials(username, password)
        self.__connection_params = pika.ConnectionParameters(
            credentials=creds, host=server)
        try:
            self.__connection = pika.BlockingConnection(
                parameters=self.__connection_params)
        except socket.gaierror:
            raise ConnectionError(f"Could not Connect to server '{server}'")
        except (ProbableAccessDeniedError, ProbableAuthenticationError, AuthenticationError):
            raise AuthenticationError(
                f"User '{username}' not authorized to connect to '{server}'")
        self.__channel = self.__connection.channel()
        self.__exchange = exchange

    @property
    def exchange(self) -> str:
        return str(self.__exchange)

    @property
    def channel(self) -> pika.channel.Channel:
        return self.__channel

    def close(self) -> None:
        self.__channel.close()
        try:
            self.__connection.close()
        except ConnectionWrongStateError:
            # connection already closed
            pass

# add some aynchronous confirm_delivery() to make sure that message was delivered
class Publisher(multiprocessing.Process):

    def __init__(self, in_queue: multiprocessing.Queue, connections: List[ServerConnection], stop_sig: EventClass) -> None:
        super().__init__(None, None, 'Process-Publisher', (), {})
        self.__input = in_queue
        self.connections = connections
        self.__stop: EventClass = stop_sig

        self.invalid_msgs: List[AMQP_MSG] = []
        self.unsent_msgs: List[AMQP_MSG] = []

    def run(self) -> None:
        while True:
            if self.__stop.is_set():
                break

            try:
                new_msg = self.__input.get(block=False)
            except Empty:
                continue

            try:
                key, payload = new_msg
                payload = json.dumps(payload)
            except Exception:
                self.invalid_msgs.append(new_msg)

            for conn in self.connections:
                conn.channel.basic_publish(conn.exchange, key, payload, pika.BasicProperties(content_type='text/json', delivery_mode=1))
                    #create timer that calls __republish
                    
        # write unsent messages to file    
    
    def __republish(connection, message, atmpt_num) -> None:
        pass
     
    def add_connection(self, new_connection: ServerConnection) -> None:
        self.connections.append(new_connection)
