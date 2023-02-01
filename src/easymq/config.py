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

# Used to verify the values that users set are valid
# Callable should raise an error if value is incorrect
VERIFY_VALUE: Dict[str, Callable] = {
    "RECONNECT_DELAY": float,
    "RECONNECT_TRIES": int,
    "DEFAULT_SERVER": str,
    "DEFAULT_EXCHANGE": str,
    "DEFAULT_USER": str,
    "DEFAULT_PASS": str,
    "DEFAULT_ROUTE_KEY": str,
    "RABBITMQ_PORT": int,
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


def load_config_from_file(cstm_cfg_file_path: str) -> None:
    try:
        with open(cstm_cfg_file_path, "r", encoding="utf-8") as cfg_file:
            _variable_dict: Dict = json.load(cfg_file)
    except json.decoder.JSONDecodeError:
        warnings.warn(
            f"file '{cstm_cfg_file_path}' has incorrectly formatted JSON, using default values instead"
        )
        return
    except IOError:
        warnings.warn(
            f"Cannot load config, file '{cstm_cfg_file_path}' doesn't exist!"
        )
        return

    # verify all values in user config file
    for _var in _variable_dict:
        try:
            valid_value = VERIFY_VALUE[_var](_variable_dict[_var])
            setattr(_cur_module, _var, valid_value)
        except AttributeError:
            warnings.warn(
                f"Skipping variable {_var} from {cstm_cfg_file_path}, not a valid config variable!"
            )
        except ValueError:
            warnings.warn(
                f"Using default value of {_var}. Value '{_variable_dict[_var]}' in {cstm_cfg_file_path} not valid!"
            )


def set_cfg_var(config_var: str, value: Any, durable=False) -> None:
    config_var = config_var.upper()
    # remove variable from config file/use default
    new_value = BASE_VALUES[config_var] if value is None else VERIFY_VALUE[config_var](value)
    setattr(_cur_module, config_var, new_value)
    CURRENT_CONFIG[config_var] = new_value
    if durable:
        with open(CONFIG_PATH, 'w') as cfg_file:
            cfg_file.write(get_current_config())


def _init() -> None:
    # setup user config file
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
