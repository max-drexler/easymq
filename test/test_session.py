import pytest

import easymq
from easymq.exceptions import NotConnectedError
from easymq.session import get_current_session


def test_auto_connect():
    easymq.publish('hello')
    assert len(get_current_session().pool.connections) == 1
    easymq.disconnect()


def test_cannot_connect_default():
    easymq.configure('DEFAULT_USER', 'incorrect_user')
    with pytest.raises(NotConnectedError):
        easymq.publish('hello')
    easymq.configure('DEFAULT_USER', None)
