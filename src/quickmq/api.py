"""
easymq.api
~~~~~~~~~~

Methods that are exposed to the user by default
"""

import atexit
from typing import Any, Iterable, Union, Tuple, Optional, Callable

from .session import AmqpSession


_CURRENT_SESSION = AmqpSession()


atexit.register(_CURRENT_SESSION.disconnect)


# Server connection API
def connect(
    *args, auth: Optional[Tuple[Optional[str], Optional[str]]] = (None, None)
) -> None:
    _CURRENT_SESSION.connect(*args, auth=auth or (None,) * 2)


def disconnect(*args) -> None:
    _CURRENT_SESSION.disconnect(*args)


# Publishing API
def publish(
    message: Any,
    key: Optional[str] = None,
    exchange: Optional[str] = None,
    confirm_delivery=True,
) -> None:
    _CURRENT_SESSION.publish(message, key, exchange, confirm_delivery)


def publish_all(
    messages: Iterable[Union[str, Tuple[str, Any]]],
    exchange: Optional[str] = None,
    confirm_delivery=True,
) -> None:
    _CURRENT_SESSION.publish_all(messages, exchange, confirm_delivery)


# Consuming API *implement later
def get(
    name: Optional[str] = None,
    key: Optional[str] = None,
    block=False,
    timeout: Optional[float] = None,
    type: str = "exchange",
) -> Union[str, None]:
    raise NotImplementedError("Coming soon to an easymq near you")


def consume(
    callback: Callable,
    name: Optional[str] = None,
    key: Optional[str] = None,
    type: str = "exchange",
) -> None:
    raise NotImplementedError("Coming soon to an easymq near you")
