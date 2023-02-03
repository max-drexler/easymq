import json
import os
import sys
import warnings
from typing import Dict, Callable, Union, Any
from platformdirs import PlatformDirs


_cur_module = sys.modules[__name__]

# When the user doesn't specify any values, these are used
BASE_VALUES: Dict[str, Union[str, int, float]] = {
    "RECONNECT_DELAY": 5.0,
    "RECONNECT_TRIES": 3,
    "DEFAULT_SERVER": "localhost",
    "DEFAULT_EXCHANGE": "",
    "DEFAULT_USER": "guest",
    "DEFAULT_PASS": "guest",
    "DEFAULT_ROUTE_KEY": "",
    "RABBITMQ_PORT": 5672,
}


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


# Used to verify the values of configuration variables
# Callable should raise ValueError if value is incorrect
VERIFY_VALUE: Dict[str, Callable] = {
    "RECONNECT_DELAY": verify_pos_float,
    "RECONNECT_TRIES": int,
    "DEFAULT_SERVER": str,
    "DEFAULT_EXCHANGE": str,
    "DEFAULT_USER": str,
    "DEFAULT_PASS": str,
    "DEFAULT_ROUTE_KEY": str,
    "RABBITMQ_PORT": verify_pos_int,
}


RECONNECT_DELAY = BASE_VALUES["RECONNECT_DELAY"]
RECONNECT_TRIES = BASE_VALUES["RECONNECT_TRIES"]
DEFAULT_SERVER = BASE_VALUES["DEFAULT_SERVER"]
DEFAULT_EXCHANGE = BASE_VALUES["DEFAULT_EXCHANGE"]
DEFAULT_USER = BASE_VALUES["DEFAULT_USER"]
DEFAULT_PASS = BASE_VALUES["DEFAULT_PASS"]
DEFAULT_ROUTE_KEY = BASE_VALUES["DEFAULT_ROUTE_KEY"]
RABBITMQ_PORT = BASE_VALUES["RABBITMQ_PORT"]

CURRENT_CONFIG = BASE_VALUES
CONFIG_PATH = None


def get_default_config() -> str:
    return json.dumps(BASE_VALUES, indent=4)


def get_current_config() -> str:
    return json.dumps(CURRENT_CONFIG, indent=4)


def load_config_from_file(cfg_file_path: str) -> None:
    try:
        with open(cfg_file_path, "r", encoding="utf-8") as cfg_file:
            new_cfg_file: Dict = json.load(cfg_file)
    except json.decoder.JSONDecodeError:
        warnings.warn(
            f"file '{cfg_file_path}' has incorrectly formatted JSON, using default values instead"
        )
        return
    except IOError as e:
        warnings.warn(
            f"Error loading config '{cfg_file_path}': [{e.errno}] {e.strerror}"
        )
        return

    for config_var in new_cfg_file:
        set_cfg_var(config_var, new_cfg_file[config_var])


def set_cfg_var(config_var: str, value: Any, durable=False) -> None:
    config_var = config_var.upper()
    try:
        new_value = BASE_VALUES[config_var] if value is None else VERIFY_VALUE[config_var](value)
    except (KeyError, AttributeError):
        warnings.warn(f"configuration variable '{config_var}' doesn't exist!")
        return
    except ValueError:
        warnings.warn(
            f"Cannot set {config_var} to '{value}', using default value instead."
        )
        return
    setattr(_cur_module, config_var, new_value)
    CURRENT_CONFIG[config_var] = new_value
    if durable:
        with open(CONFIG_PATH, 'w') as cfg_file:
            cfg_file.write(get_current_config())


def _init() -> None:
    # setup user config file in the user directory
    usr_cfg_dir = PlatformDirs("easymq").user_config_dir
    os.makedirs(usr_cfg_dir, exist_ok=True)
    usr_cfg_file = os.path.join(usr_cfg_dir, 'variables.json')
    if not os.path.isfile(usr_cfg_file):
        with open(usr_cfg_file, 'w') as cfg_file:
            cfg_file.write(get_default_config())

    global CONFIG_PATH
    CONFIG_PATH = os.environ.get("EASYMQ_CONFIG", usr_cfg_file)
    load_config_from_file(CONFIG_PATH)


_init()
