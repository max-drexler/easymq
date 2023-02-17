import pytest
import easymq
from easymq.session import get_current_session
import json


@pytest.fixture(autouse=True)
def start_easymq():
    easymq.connect('localhost')
    yield
    easymq.disconnect()


def test_connection():
    easymq.connect('localhost')
    assert len(get_current_session().pool.connections) == 1
    easymq.disconnect()
    assert len(get_current_session().pool.connections) == 0


@pytest.mark.parametrize('exchange', ['amq.fanout'])
def test_publish(create_listener):
    msg = "Hello World!"
    easymq.publish(message=msg, exchange='amq.fanout', block=True)
    rcvd_bytes = create_listener.get_message(block=True)
    assert json.loads(rcvd_bytes) == msg


def test_consume():
    def _clbk():
        pass
    with pytest.raises(NotImplementedError):
        easymq.consume(_clbk)


def test_get():
    with pytest.raises(NotImplementedError):
        easymq.get()


@pytest.mark.parametrize('exchange', ['amq.fanout'])
def test_publish_all(create_listener):
    msgs = ["Hello", "World!"]
    easymq.publish_all(msgs, exchange='amq.fanout', block=True)
    for msg in msgs:
        assert json.loads(create_listener.get_message(block=True)) == msg


def test_publish_non_exchange():
    with pytest.raises(Exception):
        easymq.publish('Test', exchange='not_existent_exchange', block=True)
    easymq.publish('test', exchange='not_existent_exchange')
