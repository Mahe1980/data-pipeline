#!/usr/bin/env python
# -*- coding: utf-8 -*-
from boto3 import client
from core.aws import AwsService

from .execution_context import ExecutionContext


class Job:
    def __init__(self, job_name: str):
        self._job = client(AwsService.Glue.value)
        self._job_name = job_name

    @staticmethod
    def _prep_arguments(execution_context: ExecutionContext):
        glue_variables = dict()
        glue_variables["--correlation_id"] = execution_context.aws_request_id
        glue_variables["--file_bucket"] = execution_context.source_bucket_name
        glue_variables["--file_key"] = execution_context.source_object_key
        glue_variables["--file_version_id"] = execution_context.source_object_version
        glue_variables["--target_bucket"] = execution_context.target_bucket_name
        glue_variables["--processed_on"] = execution_context.event_timestamp
        glue_variables["--business_process"] = execution_context.business_process
        glue_variables["--pipeline_name"] = execution_context.pipeline_name
        glue_variables["--business_email"] = execution_context.business_email_to
        glue_variables["--support_email"] = execution_context.support_email_to
        glue_variables["--account_number"] = execution_context.aws_account_number
        glue_variables["--error_details_crawler"] = execution_context.error_details_crawler
        glue_variables["--error_summary_crawler"] = execution_context.error_summary_crawler

        return glue_variables

    def run_job(self, execution_context: ExecutionContext):
        glue_variables = self._prep_arguments(execution_context)
        res = self._job.start_job_run(
            JobName=self._job_name, Arguments=glue_variables)
        return res
