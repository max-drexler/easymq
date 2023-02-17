import pytest
import quickmq
from quickmq.session import get_current_session
import json


@pytest.fixture(autouse=True)
def start_easymq():
    quickmq.connect('localhost')
    yield
    quickmq.disconnect()


def test_connection():
    quickmq.connect('localhost')
    assert len(get_current_session().pool.connections) == 1
    quickmq.disconnect()
    assert len(get_current_session().pool.connections) == 0


@pytest.mark.parametrize('exchange', ['amq.fanout'])
def test_publish(create_listener):
    msg = "Hello World!"
    quickmq.publish(message=msg, exchange='amq.fanout', block=True)
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
def test_publish_all(create_listener):
    msgs = ["Hello", "World!"]
    quickmq.publish_all(msgs, exchange='amq.fanout', block=True)
    for msg in msgs:
        assert json.loads(create_listener.get_message(block=True)) == msg


def test_publish_non_exchange():
    with pytest.raises(Exception):
        quickmq.publish('Test', exchange='not_existent_exchange', block=True)
    quickmq.publish('test', exchange='not_existent_exchange')
