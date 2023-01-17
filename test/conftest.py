import pytest
from .amqp_server import TestAmqpServer

TEST_SERVER = TestAmqpServer()

@pytest.fixture(scope="session")
def get_server():
    return TEST_SERVER

@pytest.fixture(autouse=True, scope="session")
def start_server():
    TEST_SERVER.start()
    yield
    TEST_SERVER.terminate()