import os
import json

import pytest

import easymq
from easymq.config import Configuration, Variable, CURRENT_CONFIG


def test_load_custom_cfg():
    test_config_path = os.path.join(os.path.dirname(__file__), "cfg/test_defaults.json")
    with pytest.warns(UserWarning):
        new_config = Configuration.load_from_file(test_config_path)
    assert new_config.get("default_user") == "test"
    assert new_config.get("default_exchange") == ""
    assert new_config.path == test_config_path


def test_bad_custom_cfg():
    with pytest.warns(UserWarning):
        Configuration.load_from_file(
            os.path.join(os.path.dirname(__file__), "cfg/bad_json.json")
        )


def test_iterable():
    try:
        iter(CURRENT_CONFIG)
    except Exception:
        pytest.fail


def test_variable():
    def verify():
        pass

    new_var = Variable("test_var", "123", verify)
    assert new_var == "test_var"
    assert new_var == new_var
    assert str(new_var) == "Config variable: test_var"


def test_missing_config():
    with pytest.warns(UserWarning):
        Configuration.load_from_file("dne.json")


def test_set_get_value():
    easymq.configure("default_exchange", "amq.fanout")
    assert easymq.configure("default_exchange") == "amq.fanout"
    easymq.configure("default_exchange", None)
    assert easymq.configure("default_exchange") == ""


def test_write_value():
    easymq.configure("default_exchange", "amq.fanout", durable=True)
    with open(easymq.config.config_file_path, encoding="utf-8") as cfg_file:
        cfg = json.load(cfg_file)
    assert cfg["DEFAULT_EXCHANGE"] == "amq.fanout"
    easymq.configure("default_exchange", None, durable=True)


def test_get_invalid_variable():
    with pytest.raises(AttributeError):
        easymq.configure("fake_variable")
