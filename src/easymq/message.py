"""
easymq.message
~~~~~~~~~~~~~~

Module that contains classes/functions for amqp message abstractions.
"""


from dataclasses import dataclass
from typing import Union, List, Dict, Any

JSONType = Union[str, int, float, bool, None, Dict[str, Any], List[Any]]


class Message:
    def __init__(self, message: Any) -> None:
        pass

    def encode():
        pass

    def decode():
        pass

    def get_properties():
        pass


@dataclass
class Packet:
    message: Message
    routing_key: str
    exchange: str
