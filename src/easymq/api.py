from typing import Iterable, Union, Tuple, Optional, Callable

from .session import get_current_session

# Server connection API
def connect(*args, credentials=None, add=False) -> None:
    get_current_session().connect(*args, credentials=credentials, add=add)


def disconnect(*args) -> None:
    get_current_session().disconnect(*args)


# Publishing API
def publish(
    message,
    key: Optional[str] = None,
    name: Optional[str] = None,
    type: str = "exchange",
    block=False,
    timeout: Optional[float] = None,
) -> None:
    pass


def publish_all(
    messages: Iterable[Union[str, Tuple[str, str]]],
    name: Optional[str] = None,
    type: str = "exchange",
    block=False,
    timeout: Optional[float] = None,
) -> None:
    pass


# Consuming API *implement later
def get(
    name: Optional[str] = None,
    key: Optional[str] = None,
    block=False,
    timeout: Optional[float] = None,
    type: str = "exchange",
) -> Union[str, None]:
    pass


def consume(
    callback: Callable,
    name: Optional[str] = None,
    key: Optional[str] = None,
    type: str = "exchange",
) -> None:
    pass
