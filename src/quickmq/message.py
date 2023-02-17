"""
easymq.message
~~~~~~~~~~~~~~

Module that contains classes/functions for amqp message abstractions.
"""


from dataclasses import dataclass
import json
from typing import Union, List, Dict, Any

import pika

JSONType = Union[str, int, float, bool, None, Dict[str, Any], List[Any]]


class Message:
    def __init__(self, message: Any) -> None:
        self._message = message

    def encode(self) -> str:
        return json.dumps(self._message)

    def decode() -> Any:
        pass

    def __str__(self) -> str:
        return f'Message: {self._message}'


@dataclass
class Packet:
    message: Message
    routing_key: str
    exchange: str
    confirm: bool = False
    properties: pika.BasicProperties = pika.BasicProperties(
        delivery_mode=1, content_type="application/json"
    )
