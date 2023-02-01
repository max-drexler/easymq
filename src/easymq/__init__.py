from .adapter import MQCredentials
from .config import set_cfg_var
from .publish import Publisher, PublisherPool
from .api import publish, consume
from .__version__ import (
    __author__,
    __version__,
)
