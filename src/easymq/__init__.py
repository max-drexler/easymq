import logging

from .__version__ import __author__, __version__
from .api import connect, consume, disconnect, get, publish, publish_all
from .config import configure
from .session import AmqpSession, _CURRENT_SESSION

__all__ = [
    "publish",
    "configure",
    "__version__",
    "__author__",
    "consume",
    "get",
    "publish_all",
    "connect",
    "disconnect",
    "AmqpSession",
]

logging.getLogger(__name__).addHandler(logging.NullHandler())
