import pytest

from easymq.adapter import PikaConnection
from pika import ConnectionParameters

def test_local(rabbitmq):
    conn = PikaConnection(ConnectionParameters(host='localhost'))
    serv_conns = rabbitmq.rabbitctl_output('list_connections').split('\n')
    assert len(serv_conns) == 2