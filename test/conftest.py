import pytest
import queue
import pika
import pika.channel
import threading
from pytest_rabbitmq import factories

AMQP_ROUTING_KEY = "*.*.*"
AMQP_TEST_EXCHANGE = 'easymq_test'

class AmqpClient:
    
    QUEUE_NAME = 'emq_standard'
    
    def __init__(self, routing_key, exchange_name, exchange_type='topic', connect_url = 'localhost', username='guest', password='guest') -> None:
        self._routing_key = routing_key
        self._exchange_name = exchange_name
        self._exchange_type = exchange_type
        
        self.should_reconnect = False
        self.was_consuming = False
        
        self._connection: pika.SelectConnection = None
        self._channel: pika.channel.Channel = None
        self._closing = False
        self._url = connect_url
        self._consuming = False
        self._prefetch_count = 1
        
        self._recv_msgs = queue.Queue()
        self._auth = pika.PlainCredentials(username, password, erase_on_connect=True)
        
    def connect(self):
        """Connect to RabbitMQ
        """
        
        return pika.SelectConnection(
            parameters=pika.ConnectionParameters(host=self._url, credentials=self._auth),
            on_open_callback=self.on_connection_open,
            on_open_error_callback=self.on_connection_open_error,
            on_close_callback=self.on_connection_closed
        )

    def close_connection(self):
        self._consuming = False
        if self._connection is None:
            return
        if not self._connection.is_closing and self._connection.is_closed:
            self._connection.close()
            
    def on_connection_open(self, _unused_connection):
        print('connection opened')
        self._connection.channel(on_open_callback=self.on_channel_open)
    
    def on_connection_closed(self, _unused_connection, reason):
        print('connection closed')
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            self.reconnect()
    
    def on_channel_open(self, channel):
        self._channel = channel
        self._channel.add_on_close_callback(self.on_channel_closed)    
        self._channel.exchange_declare(
            exchange=self._exchange_name,
            exchange_type=self._exchange_type,
            callback=self.on_exchange_declareok
        )
    
    def on_channel_closed(self, channel, reason):
        self.close_connection()
    
    def reconnect(self):
        self.should_reconnect = True
        self.stop()
    
    def on_exchange_declareok(self):
        """setup queue
        """
        self._channel.queue_declare(queue=self.QUEUE_NAME, callback=self.on_queue_declareok)
        
    def on_queue_declareok(self, _unused_frame):
        self._channel.queue_bind(
            self.QUEUE_NAME,
            self._exchange_name,
            routing_key=self._routing_key,
            callback=self.on_bindok
        )
        
    def on_bindok(self, _unused_frame):
        self._channel.basic_qos(
            prefetch_count=self._prefetch_count, callback=self.on_basic_qos_ok
        )
        
    def on_basic_qos_ok(self, _unused_frame):
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)
        self._consumer_tag = self._channel.basic_consume(
            self.QUEUE_NAME, self.on_message
        )
        self.was_consuming = True
        self._consuming = True
        
    def on_consumer_cancelled(self, method_frame):
        if self._channel:
            self._channel.close()
    
    def on_message(self, _unused_channel, basic_deliver, properties, body):
        self._recv_msgs.put(body)
        self._channel.basic_ack(basic_deliver.delivery_tag)
    
    def stop_consuming(self):
        if self._channel:
            self._channel.basic_cancel(self._consumer_tag, self.on_cancelok)

    def on_cancelok(self, _unused_frame):
        self._consuming = False
        self.close_channel()
        
    def close_channel(self):
        self._channel.close()
        
    def on_connection_open_error(self):
        print('error while connecting')
        self.reconnect()
    
    def get_message(self, block=True):
        try:
            return self._recv_msgs.get(block=block)
        except queue.Empty:
            return None
    
    def stop(self):
        if not self._closing:
            self._closing = True
            if not self._connection:
                return
            if self._consuming:
                self.stop_consuming()
                self._connection.ioloop.start()
            else:
                self._connection.ioloop.start()


rabbitmq = factories.rabbitmq_proc(ctl='/usr/sbin/rabbitmqctl', server='/usr/sbin/rabbitmq-server')

@pytest.fixture(autouse=True, scope="session")
def durable_consumer():
    client = AmqpClient(AMQP_ROUTING_KEY, AMQP_TEST_EXCHANGE)
    cl_thread = threading.Thread(target=client.connect)
    cl_thread.start()
    yield client
    client.stop()
    cl_thread.join()

@pytest.fixture
def consumer():
    client = AmqpClient(AMQP_ROUTING_KEY, AMQP_TEST_EXCHANGE)
    cl_thread = threading.Thread(target=client.connect)
    cl_thread.start()
    yield client
    client.stop()
    cl_thread.join()
    
    
@pytest.fixture
def remote_consumer(server):
    client = AmqpClient(AMQP_ROUTING_KEY, AMQP_TEST_EXCHANGE, connect_url=server.param)
    cl_thread = threading.Thread(target=client.connect)
    cl_thread.start()
    yield client
    client.stop()
    cl_thread.join()