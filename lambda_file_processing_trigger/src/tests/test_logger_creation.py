from pytest import mark

from lambda_file_processing_trigger.src.event_processor.logger import Logger

test_log_levels = [("INFO", 20, True),
                   ("inFo", 20, True),
                   ("DEBUG", 10, True),
                   ("WARNING", 30, True),
                   ("Error", 40, True),
                   ("CRItICaL", 50, True),
                   ("BLABLA", 20, True)]

test_log_level_update = [("INFO", "DEBUG", 10, True),
                         ("inFo", "InFO", 20, True),
                         ("ERROR", "BLABLA", 20, True)]


# FIXME make the loggings creation simpler
@mark.parametrize("logging_level, expected, assertion", test_log_levels)
def test_logger_info(logging_level, expected, assertion):
    log = Logger(message_format=["message"],
                 log_level=logging_level).create_logger()
    assert (log.level == expected) == assertion


@mark.parametrize("log_level, new_level, expected, assertion", test_log_level_update)
def test_logger_update_log_level(log_level, new_level, expected, assertion):
    logger = Logger(message_format=["message"], log_level=log_level)
    log = logger.create_logger()
    logger.update_level(logger=log, new_level=new_level)
    assert (log.level == expected) == assertion
