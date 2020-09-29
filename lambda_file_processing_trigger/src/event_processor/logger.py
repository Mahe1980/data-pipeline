from typing import List

from loggings.factory import LoggerFactory

from .enumerator import LogEnum


class Logger:
    def __init__(self, message_format: List, log_level: str):
        self._message_format = message_format
        self._log_level = self._get_log_level(log_level=log_level)

    @staticmethod
    def _get_log_level(log_level) -> str:
        try:
            return LogEnum[log_level.upper()].value
        except KeyError:
            return LogEnum.INFO.value

    def create_logger(self):
        config = {"msg_format": self._message_format, "level": self._log_level}
        return LoggerFactory().create_json_logger(**config)

    def update_level(self, logger, new_level):
        logger.setLevel(self._get_log_level(new_level))
