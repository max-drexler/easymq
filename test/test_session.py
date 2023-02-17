import pytest

import quickmq
from quickmq.exceptions import NotConnectedError
from quickmq.session import get_current_session, AmqpSession, exit_handler


def test_auto_connect():
    quickmq.publish("hello")
    assert len(get_current_session().pool.connections) == 1
    quickmq.disconnect()


def test_cannot_connect_default():
    quickmq.configure("DEFAULT_USER", "incorrect_user")
    with pytest.raises(NotConnectedError):
        quickmq.publish("hello")
    quickmq.configure("DEFAULT_USER", None)


def test_disconnect_args():
    quickmq.connect("localhost")
    assert len(get_current_session().pool.connections) == 1
    quickmq.disconnect("localhost")
    assert len(get_current_session().pool.connections) == 0


def test_deletion():
    new_session = AmqpSession()
    new_session.connect("localhost")
    del new_session


def test_auto_disconnect():
    quickmq.connect("localhost")
    assert len(get_current_session().servers) == 1
    exit_handler()
    assert len(get_current_session().servers) == 0
