"""
quickmq.structures
~~~~~~~~~~~~~~~~~~

Structures that power QuickMQ.
"""

from contextlib import contextmanager
import logging
import threading
from typing import Optional, Generator, Callable

LOGGER = logging.getLogger("quickmq")

"""
publish:
    connection.sched_action()

"""


class ConnectionSyncMixin:
    def __init__(self) -> None:
        self._conn_processing = threading.Event()
        self._conn_processing.set()  # not processing, don't want threads to block
        self._conn_error: Optional[Exception] = None

    @contextmanager
    def sync_to_connection(
        self, to_raise: Optional[Exception] = None
    ) -> Generator[None, None, None]:
        self._conn_processing.clear()
        yield
        LOGGER.debug("Waiting for connection thread to finish publishing")
        self._conn_processing.wait()
        if self._conn_error is None:
            return
        LOGGER.warning(f"Error detected while publishing: {self._conn_error}")
        err = to_raise or self._conn_error
        self._conn_error = None
        raise err

    def sync_caller(self, func):
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                self._conn_error = e
            finally:
                self._conn_processing.set()

        return wrapper

    @contextmanager
    def sync_to_caller(self) -> Generator[None, None, None]:
        try:
            yield
        except Exception as e:
            self._conn_error = e
            LOGGER.error(f"Error occured in connection callback {e}")
        finally:
            self._conn_processing.set()

    def await_action(self, action: Callable, *args, **kwargs):
        sync_action = self.sync_caller(action)
        with self.sync_to_connection():
            return sync_action(*args, **kwargs)
