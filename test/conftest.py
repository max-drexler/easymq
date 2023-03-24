from typing import Optional
import pytest
import queue
import pika
import pika.channel
import threading


AMQP_ROUTING_KEY = ""
AMQP_TEST_EXCHANGE = "easymq_test"


class AmqpListener(threading.Thread):
    def __init__(
        self,
        con_paramaters: pika.ConnectionParameters,
        exchange=AMQP_ROUTING_KEY,
        key=AMQP_ROUTING_KEY,
    ) -> None:
        super().__init__(None, None, "Amqp-Listener", None, None)

        self._is_running = False
        self._messages = queue.Queue()
        self._connection = None
        self._channel = None
        self._tag = None

        self._connect(con_paramaters)
        self._topo_setup(exchange, key)
        self.start()

    @property
    def running(self) -> bool:
        return self._is_running

    def _topo_setup(self, exchange_name: str, route_key: str) -> None:
        if self._channel is None or self._channel.is_closed:
            raise ConnectionError("Channel closed")
        self._channel.queue_declare("easymq_testq")
        self._channel.queue_bind(
            queue="easymq_testq", exchange=exchange_name, routing_key=route_key
        )
        self._tag = self._channel.basic_consume(
            queue="easymq_testq", on_message_callback=self._msg_received, auto_ack=True
        )

    def _connect(self, con_parmaeters: pika.ConnectionParameters) -> None:
        self._connection = pika.BlockingConnection(con_parmaeters)
        self._channel = self._connection.channel()

    def get_message(self, block=False, timeout: Optional[float] = None):
        try:
            return self._messages.get(block=block, timeout=timeout)
        except queue.Empty:
            return None

    def _msg_received(self, ch, method_frame, _header_frame, body):
        self._messages.put(body)

    def run(self) -> None:
        self._is_running = True
        while self._is_running:
            self._connection.process_data_events(time_limit=1)

    def _close(self):
        self._channel.basic_cancel(self._tag)
        self._connection.process_data_events()
        if self._channel.is_open:
            self._channel.close()
        if self._connection.is_open:
            self._connection.close()

    def close(self):
        self._is_running = False
        self._connection.add_callback_threadsafe(self._close)
        self.join()


@pytest.fixture
def rabbitmq_connection_params():
    return pika.ConnectionParameters(
        host="localhost", credentials=pika.PlainCredentials("guest", "guest")
    )


@pytest.fixture
def create_listener(rabbitmq_connection_params, exchange) -> AmqpListener:
    listener: AmqpListener = AmqpListener(rabbitmq_connection_params, exchange)
    yield listener
    listener.close()


@pytest.fixture(scope="session")
def default_listener(create_listener) -> AmqpListener:
    listener = create_listener("", None)
    yield listener
