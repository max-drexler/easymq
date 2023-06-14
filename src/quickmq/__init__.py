import logging

from .__version__ import __author__, __version__
from .api import connect, consume, disconnect, get, publish, edit
from .config import configure
from .message import Message, Packet
from .client import AmqpClient

__all__ = [
    "publish",
    "configure",
    "__version__",
    "__author__",
    "consume",
    "get",
    "connect",
    "edit",
    "disconnect",
    "AmqpClient",
    "Packet",
    "Message",
]

logging.getLogger("quickmq").addHandler(logging.NullHandler())
