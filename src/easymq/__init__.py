import atexit

from .__version__ import __author__, __version__
from .api import connect, consume, disconnect, get, publish, publish_all
from .config import configure
from .session import AmqpSession


__all__ = [
    "publish",
    "configure",
    "__version__",
    "consume",
    "get",
    "publish_all",
    "connect",
    "disconnect",
    "AmqpSession",
]

# atexit.register(getattr())
