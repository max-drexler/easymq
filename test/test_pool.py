from easymq import PublisherPool
import pytest
from pika.exceptions import AuthenticationError
from conftest import AMQP_ROUTING_KEY, AMQP_TEST_EXCHANGE

TEST_MSG = (AMQP_ROUTING_KEY.replace('*','test'), {'received': 'received'})

def test_false_server():
    with pytest.raises(ConnectionError):
        client = PublisherPool().connect('this.server.does.not.exist')
        
def test_no_credentials():
    with pytest.raises(AuthenticationError):
        client = PublisherPool().connect('dcprod-dev.ssec.wisc.edu')
        
def test_context_manager():
    with PublisherPool().connect('localhost') as client:
        assert client.open_connections == ['localhost']
    assert client.open_connections == []

@pytest.mark.skip
def test_local_msg_send(durable_consumer):
    client = PublisherPool().connect('localhost')
    client.exchange = AMQP_TEST_EXCHANGE
    client.publish(TEST_MSG)
    
    assert durable_consumer.get_message() == TEST_MSG[1]
