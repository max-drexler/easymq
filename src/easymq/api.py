from typing import Iterable, Union, Tuple, Optional, Callable

from .session import get_current_session


# Server connection API
def connect(*args, auth: Tuple[Optional[str], Optional[str]] = (None, None)) -> None:
    get_current_session().connect(*args, auth=auth)


def disconnect(*args) -> None:
    get_current_session().disconnect(*args)


def connect_url(*args) -> None:
    pass


# Publishing API
def publish(
    message: str,
    key: Optional[str] = None,
    name: Optional[str] = None,
    block=False,
    timeout: Optional[float] = None,
) -> None:
    pass


def publish_all(
    messages: Iterable[Union[str, Tuple[str, str]]],
    name: Optional[str] = None,
    block=False,
    timeout: Optional[float] = None,
) -> None:
    pass


def publish_to_queue(
    message: str,
    key: Optional[str] = None,
    name: Optional[str] = None,
    block=False,
    timeout: Optional[float] = None,
):
    pass


def publish_all_to_queue(
    message: str,
    key: Optional[str] = None,
    name: Optional[str] = None,
    block=False,
    timeout: Optional[float] = None,
):
    pass


# Consuming API *implement later
def get(
    name: Optional[str] = None,
    key: Optional[str] = None,
    block=False,
    timeout: Optional[float] = None,
    type: str = "exchange",
) -> Union[str, None]:
    raise NotImplementedError


def consume(
    callback: Callable,
    name: Optional[str] = None,
    key: Optional[str] = None,
    type: str = "exchange",
) -> None:
    raise NotImplementedError
