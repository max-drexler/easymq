from easymq import MQClient
import pytest
from pika.exceptions import AuthenticationError

def test_creation():
    pass

def test_false_server():
    with pytest.raises(ConnectionError):
        client = MQClient('model', 'this.server.does.not.exist')
        
def test_no_credentials():
    with pytest.raises(AuthenticationError):
        client = MQClient('model', 'dcprod-dev.ssec.wisc.edu')
        
def test_context_manager():
    with MQClient('model', 'dcprod-dev.ssec.wisc.edu') as client:
        pass
    assert client.connections == []
        