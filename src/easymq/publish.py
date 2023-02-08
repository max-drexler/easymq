from typing import Tuple

import pika

from easymq.connection import ConnectionPool, ServerConnection

from .message import Packet


class AmqpPublisher:
    def __init__(self) -> None:
        self._publish_props = pika.BasicProperties(
            delivery_mode=1, content_type="application/json"
        )

    def __publish(self, connection: ServerConnection, message: Tuple[str, str]) -> None:
        with connection.prepare_connection():
            connection._channel.basic_publish(
                self.__exchange,
                routing_key=message[1],
                body=message[0],
                properties=self._publish_props,
            )

    def publish_to_connection(self, connection: ServerConnection, pckt: Packet, confirm_delivery=False) -> None:
        connection.add_callback(self.__publish, connection, pckt)

    def publish_to_pool(self, pool: ConnectionPool, pckt: Packet, confirm_delivery=False) -> None:
        for con in pool:
            self.publish_to_connection(con, pckt)
