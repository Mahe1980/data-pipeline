import os

from core.aws import AwsService

from .enumerator import EnvironmentVariables, Event


class ExecutionContext:
    def __init__(self, lambda_context, event: dict):
        self._init_os_variables()
        self._init_lambda_variables(lambda_context)
        self._init_event_variables(event)

    def _init_lambda_variables(self, lambda_context):
        self._aws_request_id = lambda_context.aws_request_id

    def _init_os_variables(self):
        self._business_process = os.getenv(
            EnvironmentVariables.BusinessProcess.value)
        self._pipeline_name = os.getenv(
            EnvironmentVariables.PipelineName.value)
        self._business_email_to = os.getenv(
            EnvironmentVariables.BusinessEmail.value)
        self._support_email_to = os.getenv(
            EnvironmentVariables.SupportEmail.value)
        self._target_bucket_name = os.getenv(
            EnvironmentVariables.TargetBucket.value)
        self._error_topic = os.getenv(EnvironmentVariables.ErrorTopic.value)
        self._job_name = os.getenv(EnvironmentVariables.JobName.value)
        self._log_level = os.getenv(EnvironmentVariables.LogLevel.value)
        self._aws_account_number = os.getenv(
            EnvironmentVariables.AwsAccountNo.value)
        self._error_details_crawler = os.getenv(
            EnvironmentVariables.ErrorDetailsCrawler.value)
        self._error_summary_crawler = os.getenv(
            EnvironmentVariables.ErrorSummaryCrawler.value)

    def _init_event_variables(self, event: dict):
        self._event = event[Event.Records.value][0]

    @property
    def business_process(self):
        return self._business_process

    @property
    def pipeline_name(self):
        return self._pipeline_name

    @property
    def business_email_to(self):
        return self._business_email_to

    @property
    def support_email_to(self):
        return self._support_email_to

    @property
    def target_bucket_name(self):
        return self._target_bucket_name

    @property
    def error_topic(self):
        return self._error_topic

    @property
    def job_name(self):
        return self._job_name

    @property
    def log_level(self):
        return self._log_level

    @property
    def aws_request_id(self):
        return self._aws_request_id

    @property
    def aws_account_number(self):
        return self._aws_account_number

    @property
    def source_bucket_name(self):
        return self._event[AwsService.S3.value][Event.Bucket.value][Event.Name.value]

    @property
    def source_object_key(self):
        return self._event[AwsService.S3.value][Event.Object.value][Event.Key.value]

    @property
    def source_object_version(self):
        return self._event[AwsService.S3.value][Event.Object.value][Event.VersionId.value]

    @property
    def event_timestamp(self):
        return self._event[Event.EventTime.value]

    @property
    def error_details_crawler(self):
        return self._error_details_crawler

    @property
    def error_summary_crawler(self):
        return self._error_summary_crawler
