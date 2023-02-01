from typing import Iterable, List, Union, Tuple
from easymq.adapter import MQCredentials
import easymq.config as cfg
from .publish import Publisher, PublisherPool


def consume():
    pass


def publish(
    messages: Union[str, Iterable[Union[Tuple[str, str], str]]],
    exchange: str = cfg.DEFAULT_EXCHANGE,
    servers: Union[str, List[str]] = cfg.DEFAULT_SERVER,
    auth: Tuple[str, str] = (cfg.DEFAULT_USER, cfg.DEFAULT_PASS),
    routing_key: str = cfg.DEFAULT_ROUTE_KEY,
):
    creds = MQCredentials(username=auth[0], password=auth[1])
    if isinstance(servers, str):
        client = Publisher().connect(servers, credentials=creds)
    else:
        client = PublisherPool(credentials=creds).connect(servers)
    if isinstance(messages, str):
        client.publish(messages, route_key=routing_key)
    else:
        client.publish_all(messages)
    client.close()
