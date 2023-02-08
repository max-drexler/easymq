"""
easymq.exceptions
~~~~~~~~~~~~~~~~~

Stores all custom exceptions raised in EasyMQ.
"""


class NotAuthenticatedError(Exception):
    """User not authenticated to connect to server"""


class NotConnectedError(ConnectionError):
    """EasyMQ is not currently connected to a server"""


class EncodingError(Exception):
    """Error when encoding a message"""

# Warnings


class UndeliveredWarning(Warning):
    """A message was not delivered to a server"""
