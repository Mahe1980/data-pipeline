#!/usr/bin/env python
# -*- coding: utf-8 -*-

from event_processor.execution_context import ExecutionContext
from event_processor.job import Job
from event_processor.logger import Logger

MESSAGE_FORMAT = ["message", "asctime", "funcName", "module", "lineno"]


def lambda_handler(event, context):
    logger = Logger(message_format=MESSAGE_FORMAT, log_level="DEBUG")
    log = logger.create_logger()
    log.info("Starting Job Call")
    try:
        log.debug("Starting variable extraction")

        execution_context = ExecutionContext(
            lambda_context=context, event=event)

        log.debug(f"Updating log level to {execution_context.log_level}")
        logger.update_level(logger=log, new_level=execution_context.log_level)

        log.debug(
            f"Initiating job {execution_context.job_name} with parameters {execution_context}")
        job = Job(job_name=execution_context.job_name)
        log.debug(f"Calling job {execution_context.job_name}")
        job.run_job(execution_context=execution_context)
        log.info(f"Successfully called {execution_context.job_name}")
    except Exception as ex:
        log.error(f"Error encountered during the job call {ex}")
        raise
