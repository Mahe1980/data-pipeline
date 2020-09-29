from typing import List

from loggings.factory import LoggerFactory

MESSAGE_FORMAT = ["message", "asctime", "funcName", "module", "lineno"]


class Logger:
    def __init__(self, message_format: List, log_level: str):
        self._message_format = message_format
        self._log_level = log_level

    def create_logger(self):
        config = {"msg_format": self._message_format, "level": self._log_level}
        return LoggerFactory().create_json_logger(**config)


def get_logger(log_level="INFO"):
    logger = Logger(message_format=MESSAGE_FORMAT, log_level=log_level)
    return logger.create_logger()
