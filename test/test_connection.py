import json
import subprocess
import time

import pytest

import easymq
from easymq.exceptions import NotAuthenticatedError
from easymq.session import get_current_session


@pytest.fixture
def disconnect_rabbitmq():
    subprocess.Popen(["make", "stop_rabbitmq"])
    time.sleep(2)
    yield


@pytest.fixture
def restart_rabbitmq():
    subprocess.Popen(["make", "start_rabbitmq"])
    time.sleep(4)
    yield


def test_incorrect_credentials():
    with pytest.raises(NotAuthenticatedError):
        easymq.connect("localhost", auth=("wrong_user", "wrong_password"))


def test_wrong_server():
    with pytest.raises(ConnectionError):
        easymq.connect("not_host")


def test_reconnect(disconnect_rabbitmq, restart_rabbitmq):
    easymq.connect("localhost")
    assert len(get_current_session().pool.connections) == 1
    disconnect_rabbitmq
    restart_rabbitmq
    assert len(get_current_session().pool.connections) == 1
    easymq.disconnect()


@pytest.mark.parametrize("exchange", ["amq.fanout"])
def pub_after_disconnect(create_listener, disconnect_rabbitmq, restart_rabbitmq):
    msg = "Hello World!"
    easymq.connect("localhost")
    disconnect_rabbitmq
    easymq.publish(msg, exchange="amq.fanout")
    restart_rabbitmq
    rcvd_bytes = create_listener.get_message(block=True)
    assert json.loads(rcvd_bytes) == msg
    easymq.disconnect()
