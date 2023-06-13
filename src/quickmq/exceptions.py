"""
easymq.exceptions
~~~~~~~~~~~~~~~~~

Stores all custom exceptions raised in EasyMQ.
"""


from typing import Optional


class NotAuthenticatedError(Exception):
    """User not authenticated to connect to server"""


class EncodingError(Exception):
    """Error when encoding a message"""


class FailedEditError(Exception):
    """Error when an edit to server(s) fails"""


class ExchangeExistsError(FailedEditError):
    """Error when an exchange already exists"""

    def __init__(self, *args: object, exchange: Optional[str] = None) -> None:
        super().__init__(
            f"Exchange {exchange} already exists!" if exchange is not None else args
        )


class ExchangeNotExistError(FailedEditError):
    """Error when an exchange doesn't exist"""

    def __init__(self, *args: object, exchange: Optional[str] = None) -> None:
        super().__init__(
            f"Exchange {exchange} doesn't exist!" if exchange is not None else args
        )


# Warnings


class UndeliveredWarning(Warning):
    """A message was not delivered to a server"""
