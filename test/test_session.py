import pytest

import quickmq
from quickmq.session import AmqpClient


def test_context_manager():
    with AmqpClient() as _:
        pass


def test_publish_context_manager():
    try:
        with AmqpClient() as session:
            quickmq.configure('default_server', 'asfasd')
            session.connect('localhost')
            session.publish('hello')
    finally:
        quickmq.configure('default_server', None)


def test_connect_context_manager():
    with AmqpClient() as session:
        session.connect('localhost')
        assert len(session._connection_pool) == 1
        connection = session._connection_pool.get_connection('localhost')
        assert connection is not None and connection.connected
    assert not connection.connected


def test_auto_connect():
    with AmqpClient() as session:
        session.publish('hello')
        assert len(session.servers) == 1


def test_cannot_connect_default():
    quickmq.configure("DEFAULT_USER", "incorrect_user")
    with pytest.raises(ConnectionError):
        quickmq.publish("hello")
    quickmq.configure("DEFAULT_USER", None)


def test_disconnect_args():
    session = AmqpClient()
    session.connect('localhost')
    assert len(session._connection_pool) == 1
    session.disconnect("localhost")
    assert len(session._connection_pool) == 0


def test_deletion():
    new_session = AmqpClient()
    new_session.connect("localhost")
    del new_session
