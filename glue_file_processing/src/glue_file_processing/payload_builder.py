import re

from core.aws import AwsRegion, AwsService
from sns.sns_mail_payload import SnsMailPayload
from sns.topics import Topic

from .logger import get_logger
from .pipeline_enum import SnsStatus, SnsMessageType
from .process_context import ProcessContext

LOGGER = get_logger()

MESSAGE = {
    "html": """<html><head></head><body><h1>{subject}</h1><p>{message}</body></html>""",
    "txt": """{subject}\r\n{message}"""
}


class PayloadBuilder:
    def __init__(self, context: ProcessContext):
        self._context = context
        self._payload = SnsMailPayload()

    def _generate_payload(self, topic: Topic, region_name: AwsRegion, status: SnsStatus, email_to: str):
        try:
            self._payload.topic = self._topic_arn(topic.value, region_name)
            self._payload.process = self._context.business_process
            self._payload.pipeline_name = self._context.pipeline_name
            email_to_list = re.split(',|;', email_to)
            email_to_list = [email.strip() for email in email_to_list]
            self._payload.email_to = email_to_list
            self._payload.correlation_id = self._context.correlation_id
            self._payload.status = status.value
        except Exception as ex:
            LOGGER.error(
                f"Error generating payload for Correlation ID '{self._context.correlation_id}'")
            LOGGER.error(ex)
            raise

    def _get_message(self, subject: str, msg: str):
        LOGGER.debug(
            f"Adding message body to the payload for correlation id '{self._payload.correlation_id}'.")
        self._payload.subject = subject
        try:
            self._payload.html = MESSAGE[SnsMessageType.Html.value].format(
                subject=subject, message=msg)
            self._payload.text = MESSAGE[SnsMessageType.Txt.value].format(
                subject=subject, message=msg)
        except Exception as ex:
            LOGGER.error(f"Error getting payload message")
            LOGGER.error(ex)
            raise

    def _topic_arn(self, topic_name: str, region_name: AwsRegion):
        topic_arn = "arn:aws:{}:{}:{}:{}".format(AwsService.Sns.value, region_name.value,
                                                 self._context.account_number, topic_name)
        return topic_arn

    def build_payload(self, topic: Topic, region_name: AwsRegion, status: SnsStatus, subject: str,
                      msg: str, email_to: str) -> SnsMailPayload:
        self._generate_payload(topic, region_name, status, email_to)
        self._get_message(subject, msg)
        return self._payload
