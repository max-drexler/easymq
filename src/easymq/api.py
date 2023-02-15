"""
easymq.api
~~~~~~~~~~

Methods that are exposed to the user by default
"""

from typing import Any, Iterable, Union, Tuple, Optional, Callable

from .session import get_current_session


# Server connection API
def connect(*args, auth: Optional[Tuple[Optional[str], Optional[str]]] = (None, None)) -> None:
    get_current_session().connect(*args, auth=auth or (None,) * 2)


def disconnect(*args) -> None:
    get_current_session().disconnect(*args)


# Publishing API
def publish(
    message: Any,
    key: Optional[str] = None,
    exchange: Optional[str] = None,
    block=False,
) -> None:
    get_current_session().publish(message, key, exchange, block)


def publish_all(
    messages: Iterable[Union[str, Tuple[str, str]]],
    exchange: Optional[str] = None,
    block=False,
) -> None:
    get_current_session().publish_all(messages, exchange, block)


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
