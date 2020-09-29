from boto3 import Session
from core.aws import AwsRegion


class BotoSession:

    @staticmethod
    def get_session(region: AwsRegion):
        return Session(region_name=region.value)
