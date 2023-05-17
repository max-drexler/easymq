from json import loads
import subprocess
import sys

import pytest

from quickmq import __version__


def run_command(input: str) -> str:
    if sys.version_info[1] < 7:
        compl = subprocess.run(input.split(' '), stdout=subprocess.PIPE)
    else:
        compl = subprocess.run(input.split(' '), capture_output=True)
    return str(compl.stdout, encoding='utf-8')


def test_version():
    assert __version__ in run_command('quickmq -V')


def test_usage():
    assert 'usage' in run_command('quickmq')


@pytest.mark.parametrize('exchange', ['amq.fanout'])
def test_publish(create_listener):
    run_command('quickmq publish -s=localhost -e=amq.fanout -m=Hello')
    rcvd_bytes = create_listener.get_message(block=True)
    assert loads(rcvd_bytes) == 'Hello'


@pytest.mark.parametrize('exchange', ['amq.fanout'])
def test_publish_from_stdin(create_listener):
    proc = subprocess.Popen(['quickmq', 'publish', '-s=localhost', '-e=amq.fanout'], stdin=subprocess.PIPE)
    try:
        proc.communicate(input=b'Hello', timeout=1)
    except subprocess.TimeoutExpired:
        proc.kill()
    rcvd_bytes = create_listener.get_message(block=True)
    if rcvd_bytes is None:
        assert False
    assert loads(rcvd_bytes) == 'Hello'
