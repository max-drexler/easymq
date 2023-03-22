from contextlib import contextmanager
import threading
from typing import Generator, Optional
import logging

from quickmq.connection import ConnectionPool, ServerConnection

from .message import Packet

# Possible problem with lock when publishing to multiple servers

LOGGER = logging.getLogger('quickmq')


class AmqpPublisher:
    def __init__(self) -> None:
        self._publishing = threading.Event()
        self._publishing.set()  # not publishing, don't want threads to block
        self._publishing_err: Optional[Exception] = None

    @contextmanager
    def sync_connection(
        self, to_raise: Optional[Exception] = None
    ) -> Generator[None, None, None]:
        self._publishing.clear()
        yield
        LOGGER.debug("Waiting for connection thread to finish publishing")
        self._publishing.wait()
        if self._publishing_err is None:
            return
        LOGGER.warning(f"Error detected while publishing: {self._publishing_err}")
        err = to_raise or self._publishing_err
        self._publishing_err = None
        raise err

    def _publish(self, connection: ServerConnection, packet: Packet) -> None:
        pub_channel = (
            connection._confirmed_channel if packet.confirm else connection._channel
        )
        try:
            with connection.wrapper():
                LOGGER.debug(
                    f"Connection to {connection.server} ready, attempting to publish to {packet.exchange} exchange"
                )
                pub_channel.basic_publish(
                    packet.exchange,
                    routing_key=packet.routing_key,
                    body=packet.message.encode(),
                    properties=packet.properties,
                )
                LOGGER.debug(f"Published {packet} to {connection.server}")
        except Exception as e:
            self._publishing_err = e
            LOGGER.warning(
                f"Couldn't publish to exchange {packet.exchange} on {connection.server} because {e}"
            )
        finally:
            self._publishing.set()

    def publish_to_connection(self, connection: ServerConnection, pckt: Packet) -> None:
        if not pckt.confirm:
            connection.add_callback(self._publish, connection, pckt)
        else:
            with self.sync_connection():
                connection.add_callback(self._publish, connection, pckt)

    def publish_to_pool(self, pool: ConnectionPool, pckt: Packet) -> None:
        for con in pool:
            self.publish_to_connection(con, pckt)
