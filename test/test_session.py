import pytest

import easymq
from easymq.exceptions import NotConnectedError
from easymq.session import get_current_session, AmqpSession, exit_handler


def test_auto_connect():
    easymq.publish('hello')
    assert len(get_current_session().pool.connections) == 1
    easymq.disconnect()


def test_cannot_connect_default():
    easymq.configure('DEFAULT_USER', 'incorrect_user')
    with pytest.raises(NotConnectedError):
        easymq.publish('hello')
    easymq.configure('DEFAULT_USER', None)


def test_disconnect_args():
    easymq.connect("localhost")
    assert len(get_current_session().pool.connections) == 1
    easymq.disconnect('localhost')
    assert len(get_current_session().pool.connections) == 0


def test_deletion():
    new_session = AmqpSession()
    new_session.connect('localhost')
    del new_session


def test_auto_disconnect():
    easymq.connect('localhost')
    assert len(get_current_session().servers) == 1
    exit_handler()
    assert len(get_current_session().servers) == 0
