"""
easymq.session
~~~~~~~~~~~~~~

This module contains objects and functions to maintain a long-term amqp session.
"""

_CURRENT_SESSION = None


class AmqpSession:
    pass


def get_current_session():
    return _CURRENT_SESSION or AmqpSession()
    # if a session exists, return that
    # if not create a session
