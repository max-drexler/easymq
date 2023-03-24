import pytest

from quickmq import Message
from quickmq.exceptions import EncodingError


def test_cant_encode():
    with pytest.raises(EncodingError):
        Message(set(['hi'])).encode()


def test_str():
    payload = 'HI'
    msg = Message(payload)
    assert payload in str(msg)


def test_byte_encode():
    payload = b'bytes'
    msg = Message(payload)
    assert msg.encode() == payload


def test_decode_encode():
    payload = 'Howdy Partner'
    byt = Message(payload).encode()
    assert Message(byt).decode() == payload
