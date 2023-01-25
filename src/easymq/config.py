from typing import Union, Dict, List, Tuple

RECONNECT_DELAY=5
RECONNECT_TRIES=5

JSONType = Union[
    str,
    int,
    float,
    bool,
    None,
    Dict[str, Union[str, bool, int, float, dict]],
    List[Union[str, bool, int, float, dict]],
]

AMQP_MSG = Tuple[str, JSONType]