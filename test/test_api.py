import threading
import pytest
import quickmq
from quickmq.api import _CURRENT_SESSION
import json


@pytest.fixture(autouse=True)
def start_easymq():
    quickmq.connect('localhost')
    yield
    quickmq.disconnect()


def test_connection():
    quickmq.connect('localhost')
    assert len(_CURRENT_SESSION._connection_pool) == 1
    quickmq.disconnect()
    assert len(_CURRENT_SESSION._connection_pool) == 0


@pytest.mark.parametrize('exchange', ['amq.fanout'])
def test_publish(create_listener):
    msg = "Hello World!"
    quickmq.publish(message=msg, exchange='amq.fanout', confirm_delivery=True)
    rcvd_bytes = create_listener.get_message(block=True)
    assert json.loads(rcvd_bytes) == msg


def test_consume():
    def _clbk():
        pass
    with pytest.raises(NotImplementedError):
        quickmq.consume(_clbk)


def test_get():
    with pytest.raises(NotImplementedError):
        quickmq.get()


@pytest.mark.parametrize('exchange', ['amq.fanout'])
def test_non_confirm_publish(create_listener):
    msg = "Hello World!"
    quickmq.publish(message=msg, exchange='amq.fanout', confirm_delivery=False)
    rcvd_bytes = create_listener.get_message(block=True)
    assert json.loads(rcvd_bytes) == msg


def test_publish_non_exchange():
    with pytest.raises(Exception):
        quickmq.publish('Test', exchange='not_existent_exchange', confirm_delivery=True)


@pytest.mark.parametrize('exchange', ['amq.fanout'])
def test_mulithreading(create_listener):
    msg = "multi-threaded publishing"
    t = threading.Thread(name='test_api:test_multithreading', target=quickmq.publish, args=(msg,),
                         kwargs={'exchange': 'amq.fanout', 'confirm_delivery': True})
    t.start()
    t.join()
    rcvd_bytes = create_listener.get_message(block=True)
    assert json.loads(rcvd_bytes) == msg
