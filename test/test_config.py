import os

import easymq
from easymq.config import Configuration


def test_load_custom_cfg():
    new_config = Configuration.load_from_file(
        os.path.join(os.path.dirname(__file__), "cfg/test_defaults.json")
    )
    assert new_config.get('default_user') == 'test'


def test_set_value():
    easymq.configure("default_exchange", "amq.fanout")
    assert easymq.configure("default_exchange") == "amq.fanout"
    easymq.configure("default_exchange", None)
    assert easymq.configure("default_exchange") == ""
