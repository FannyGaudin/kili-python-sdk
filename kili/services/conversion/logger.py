"""
Logging helper
"""

import logging


class CustomAdapter(logging.LoggerAdapter):
    """
    This adapter expects the passed in dict-like object to have a
    'email' key, whose value in brackets is prepended to the log message.
    """

    def process(self, msg, kwargs):
        return f'[{self.extra["email"]}] {msg}', kwargs


def get_logger(email):
    """
    Returns a logger with custom information
    """
    logger = logging.getLogger("uvicorn")
    adapter = CustomAdapter(logger, {"email": email})
    return adapter
