from contextlib import contextmanager
import threading
from typing import Optional
from enum import Enum, auto

from pika.exceptions import ChannelClosedByBroker


class ExchangeTypes(Enum):
    DIRECT = auto(),
    TOPIC = auto(),
    FANOUT = auto(),
    HEADER = auto(),


class TopologyEditor:
    def __init__(self) -> None:
        self._processing = threading.Event()
        self._processing_error = None

    @contextmanager
    def _sync_caller(self) -> None:
        try:
            self._server_conn.wait_for_reconnect()
            yield
        except ChannelClosedByBroker as e:
            self._processing_error = e
            self._server_conn._reconnect_channel()
        except Exception as e:
            self._processing_error = e
        finally:
            self._processing.set()

    @contextmanager
    def sync_connection(self) -> None:
        try:
            self._server_conn.wait_for_reconnect()
            yield
        finally:
            self._wait_for_processing()

    def check_exchange(self, new_exchange: str) -> bool:
        self._server_conn.wait_for_reconnect()
        self._server_conn.add_callback(
            self.__declare_exchange, new_exchange, passive=True
        )
        try:
            self._wait_for_processing()
        except Exception:
            return False
        return True

    def __declare_exchange(
        self,
        exchange_name: str,
        exchange_type: str = "direct",
        durable=False,
        auto_delete=False,
        internal=False,
        passive=False,
    ) -> None:
        with self._sync_caller():
            self._server_conn._channel.exchange_declare(
                exchange_name,
                exchange_type=exchange_type,
                durable=durable,
                auto_delete=auto_delete,
                internal=internal,
                passive=passive,
            )

    def exchange_declare(
        self,
        exchange_name: str,
        exchange_type: str = "direct",
        durable=False,
        auto_delete=False,
        internal=False,
    ) -> None:
        with self.sync_connection():
            self._server_conn.add_callback(
                self.__declare_exchange,
                str(exchange_name),
                exchange_type=exchange_type,
                durable=durable,
                auto_delete=auto_delete,
                internal=internal,
            )

    def __declare_queue(
        self,
        queue_name: str,
        passive=False,
        durable=False,
        exclusive=False,
        auto_delete=False,
    ) -> None:
        with self._sync_caller():
            self._server_conn._channel.queue_declare(
                queue_name,
                passive=passive,
                durable=durable,
                exclusive=exclusive,
                auto_delete=auto_delete,
            )

    def queue_declare(
        self, queue_name: str, durable=False, exclusive=False, auto_delete=False
    ) -> None:
        with self.sync_connection():
            self._server_conn.add_callback(
                self.__declare_queue,
                queue_name,
                passive=False,
                durable=durable,
                exclusive=exclusive,
                auto_delete=auto_delete,
            )

    def queue_check(self, queue_name: str) -> bool:
        self._server_conn.wait_for_reconnect()
        self._server_conn.add_callback(self.__declare_queue, queue_name, passive=True)
        try:
            self._wait_for_processing()
        except Exception:
            return False
        return True

    def __delete_queue(self, queue_name: str, if_unused=False, if_empty=False) -> None:
        with self._sync_caller():
            self._server_conn._channel.queue_delete(
                queue_name, if_unused=if_unused, if_empty=if_empty
            )

    def queue_delete(self, queue_name: str, if_unused=False, if_empty=False) -> None:
        with self.sync_connection():
            self._server_conn.add_callback(
                self.__delete_queue, queue_name, if_unused=if_unused, if_empty=if_empty
            )

    def __delete_exchange(self, exchange_name: str, if_unused=False) -> None:
        with self._sync_caller():
            self._server_conn._channel.exchange_delete(
                exchange_name, if_unused=if_unused
            )

    def exchange_delete(self, exchange_name: str, if_unused=False) -> None:
        with self.sync_connection():
            self._server_conn.add_callback(
                self.__delete_exchange, exchange_name, if_unused=if_unused
            )

    def __bind_queue(
        self, queue_name: str, exchange_name: str, routing_key: Optional[str] = None
    ) -> None:
        with self._sync_caller():
            self._server_conn._channel.queue_bind(
                queue_name, exchange_name, routing_key=routing_key
            )

    def queue_bind(
        self, queue_name: str, exchange_name: str, routing_key: Optional[str] = None
    ) -> None:
        with self.sync_connection():
            self._server_conn.add_callback(
                self.__bind_queue, queue_name, exchange_name, routing_key=routing_key
            )

    def __unbind_queue(
        self,
        queue_name: str,
        exchange_name: str,
        routing_key: Optional[str] = None,
    ) -> None:
        with self._sync_caller():
            self._server_conn._channel.queue_unbind(
                queue_name, exchange_name, routing_key=routing_key
            )

    def queue_unbind(
        self,
        queue_name: str,
        exchange_name: str,
        routing_key: Optional[str] = None,
    ) -> None:
        with self.sync_connection():
            self._server_conn.add_callback(
                self.__unbind_queue,
                queue_name,
                exchange_name,
                routing_key=routing_key,
            )

    def __purge_queue(self, queue_name: str) -> None:
        with self._sync_caller():
            self._server_conn._channel.queue_purge(queue_name)

    def queue_purge(self, queue_name: str) -> None:
        with self.sync_connection():
            self._server_conn.add_callback(self.__purge_queue, queue_name)

    def _wait_for_processing(self, to_raise: Optional[Exception] = None) -> None:
        """waits for the pika connection to finish whatever processing it's doing
            and raises any exceptions that occur in the backend in the caller thread.

        Args:
            to_raise (Optional[Exception], optional):
                Custom exception to raise if exception was detected in pika backend. Defaults to None.

        Raises:
            to_raise: Either the exception that occurred in the backend or the custom exception
        """
        self._processing.wait()
        self._processing.clear()
        if self._processing_error is None:
            return
        err = to_raise or self._processing_error
        self._processing_error = None
        raise err

    def __del__(self) -> None:
        self.close()
        self._server_conn = None

    def __enter__(self):
        return self

    def __exit__(self) -> None:
        self.close()

    def close(self) -> None:
        if self._server_conn is not None:
            self._server_conn.close()
