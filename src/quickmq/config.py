from dataclasses import dataclass, field
import json
import os
import warnings
from typing import Dict, Callable, List, Tuple, Union, Any
from platformdirs import PlatformDirs
import logging

LOGGER = logging.getLogger('quickmq')

CFG_VALS = Union[str, int, float]
CFG_FILE_NAME = "cfg_vars.json"
CFG_FILE_PATH = os.path.join(
    PlatformDirs("easymq").user_config_dir, CFG_FILE_NAME
)


def verify_pos_float(_obj: Any) -> float:
    new_float = float(_obj)
    if new_float < 0:
        raise ValueError
    return new_float


def verify_pos_int(_obj: Any) -> int:
    new_int = int(_obj)
    if new_int < 0:
        raise ValueError
    return new_int


def verify_msg_buf_policy(_obj: Any) -> int:
    new_policy = int(_obj)
    # 1 = drop all messages when server is reconnecting
    # 2 = buffer messages when server is reconnecting and publish after reconnect
    # 3 = block until server reconnects
    # 4 = raise an error if server is disconnected
    return new_policy


@dataclass
class ConfigVariable:
    name: str
    default_value: CFG_VALS
    verify_func: Callable
    current_value: CFG_VALS = field(init=False)

    def __post_init__(self):
        self.current_value = self.default_value

    def __eq__(self, __obj: Any) -> bool:
        if isinstance(__obj, str):
            return self.name == __obj
        rep = (self.name, self.current_value)
        return rep.__eq__(__obj)

    def __str__(self) -> str:
        return f"Config variable: {self.name}"


class Configuration:

    DEFAULT_VARIABLES: List[Tuple[str, CFG_VALS, Callable]] = [
        ("RECONNECT_DELAY", 5.0, verify_pos_float),
        ("RECONNECT_TRIES", 3, int),
        ("DEFAULT_SERVER", "localhost", str),
        ("DEFAULT_EXCHANGE", "", str),
        ("DEFAULT_USER", "guest", str),
        ("DEFAULT_PASS", "guest", str),
        ("DEFAULT_ROUTE_KEY", "", str),
        ("RABBITMQ_PORT", 5672, verify_pos_int),
    ]

    def __init__(self, _config_file_path=None) -> None:
        self._variables = {
            v.name: v
            for v in [ConfigVariable(*args) for args in Configuration.DEFAULT_VARIABLES]
        }
        self._config_file_path = _config_file_path or CFG_FILE_PATH

    @property
    def path(self) -> str:
        return self._config_file_path

    @property
    def json(self) -> str:
        return json.dumps(
            {v.name: v.current_value for v in self._variables.values()}, indent=4
        )

    @property
    def default_json(self) -> str:
        return json.dumps(
            {v.name: v.default_value for v in self._variables.values()}, indent=4
        )

    @classmethod
    def load_from_file(cls, file_path: str):
        new_configuration = Configuration(file_path)
        try:
            with open(file_path, "r", encoding="utf-8") as cfg_file:
                new_cfg_file: Dict = json.load(cfg_file)
        except json.decoder.JSONDecodeError:
            LOGGER.warning(f"Bad JSON in config file {file_path}")
            warnings.warn(f"Couldn't load config from '{file_path}' bad JSON!")
            return new_configuration
        except IOError as e:
            if file_path != CFG_FILE_PATH:
                LOGGER.warning(f"IOError loading file {file_path}. {e.strerror}")
                warnings.warn(
                    f"Couldn't load config from '{file_path}' because of {e} using default values"
                )
            return new_configuration

        for (k, v) in new_cfg_file.items():
            try:
                new_configuration.set(k, v)
            except (AttributeError, ValueError) as e:
                LOGGER.warning(f"Error with variable {k}: {e}")
                warnings.warn(f"Couldn't set variable '{k}' because '{e}'")
        return new_configuration

    def set(self, variable_name: str, value: Any, durable=False) -> None:
        variable = self._variables.get(variable_name)
        if variable is None:
            raise AttributeError(f"Variable '{variable_name} doesn't exist")
        new_value = (
            variable.default_value if value is None else variable.verify_func(value)
        )
        variable.current_value = new_value
        if durable:
            self._write()

    def get(self, variable_name: str) -> CFG_VALS:
        variable = self._variables.get(variable_name.upper())
        if variable is None:
            raise AttributeError(f"Variable '{variable_name} doesn't exist")
        return variable.current_value

    def _write(self) -> None:
        os.makedirs(os.path.dirname(self._config_file_path), exist_ok=True)
        with open(self._config_file_path, "w", encoding="utf-8") as cfg:
            cfg.write(self.json)

    def __iter__(self):
        return iter(self._variables)


CURRENT_CONFIG = Configuration.load_from_file(
    file_path=os.getenv("EASYMQ_CONFIG", CFG_FILE_PATH)
)


def configure(variable_name: str, *args, durable=False) -> Union[None, CFG_VALS]:
    variable_name = variable_name.upper()
    if not args:
        return CURRENT_CONFIG.get(variable_name)
    else:
        return CURRENT_CONFIG.set(variable_name, args[0], durable=durable)
