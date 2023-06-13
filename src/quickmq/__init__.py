import logging

from .__version__ import __author__, __version__
from .api import connect, consume, disconnect, get, publish
from .config import configure
from .message import Message, Packet
from .session import AmqpSession

__all__ = [
    "publish",
    "configure",
    "__version__",
    "__author__",
    "consume",
    "get",
    "connect",
    "disconnect",
    "AmqpSession",
    "Packet",
    "Message",
]

logging.getLogger("quickmq").addHandler(logging.NullHandler())
