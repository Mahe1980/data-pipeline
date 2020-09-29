from boto3 import Session
from sns.sns_mail_payload import SnsMailPayload
from sns.sns_notification import SnsNotification

from .logger import get_logger

LOGGER = get_logger()


class Notification:
    @staticmethod
    def notification(session: Session, payload: SnsMailPayload, correlation_id: str):
        notifier = SnsNotification()
        try:
            LOGGER.info(f"Sending notification to the topic '{payload.topic}' for "
                        f"correlation id '{correlation_id}'")
            notifier.send_notification(
                session=session, sns_payload=payload)
        except Exception as ex:
            LOGGER.error(
                f"Error sending notification for Correlation ID '{correlation_id}'")
            LOGGER.error(ex)
            raise
