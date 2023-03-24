from json import loads
import subprocess

import pytest

from quickmq import __version__


def test_version():
    vInfo = subprocess.run(['quickmq', '-V'], capture_output=True)
    assert __version__ in str(vInfo.stdout, encoding='utf-8')


def test_usage():
    info = subprocess.run(['quickmq'], capture_output=True)
    assert 'usage' in str(info.stdout, encoding='utf-8')


@pytest.mark.parametrize('exchange', ['amq.fanout'])
def test_publish(create_listener):
    subprocess.run(['quickmq', 'publish', '-s=localhost', '-e=amq.fanout', '-m=Hello'])
    rcvd_bytes = create_listener.get_message(block=False)
    assert loads(rcvd_bytes) == 'Hello'


@pytest.mark.parametrize('exchange', ['amq.fanout'])
def test_publish_from_stdin(create_listener):
    proc = subprocess.Popen(['quickmq', 'publish', '-s=localhost', '-e=amq.fanout'], stdin=subprocess.PIPE)
    try:
        proc.communicate(input=b'Hello', timeout=.1)
    except subprocess.TimeoutExpired:
        proc.kill()
    rcvd_bytes = create_listener.get_message(block=False)
    assert loads(rcvd_bytes) == 'Hello'
