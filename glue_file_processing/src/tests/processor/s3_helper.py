import boto3
from core.aws import AwsService
from moto import mock_s3, mock_sns


@mock_s3
@mock_sns
class S3Helpers:
    @staticmethod
    def create_bucket(file_bucket: str):
        client = boto3.resource(AwsService.S3.value)
        if not client.Bucket(file_bucket) in client.buckets.all():
            client.create_bucket(Bucket=file_bucket)
            bucket_version = client.BucketVersioning(file_bucket)
            bucket_version.enable()

    @staticmethod
    def create_file_key(file_bucket: str, file_key: str, file_data: str):
        client = boto3.resource(AwsService.S3.value)
        obj = client.Object(file_bucket, file_key)
        return obj.put(Body=file_data)

    @staticmethod
    def get_tagging(file_bucket: str, file_key: str):
        client = boto3.client(AwsService.S3.value)
        response = client.get_object_tagging(Bucket=file_bucket, Key=file_key)
        tags = {tag['Key']: tag['Value'] for tag in response["TagSet"]}
        return tags

    @staticmethod
    def s3_key_exists(file_bucket: str, file_key: str):
        client = boto3.resource(AwsService.S3.value)
        try:
            client.Object(file_bucket, file_key).load()
        except Exception:
            return False
        else:
            return True

    @staticmethod
    def create_topic(topic_name: str, region_name: str):
        client = boto3.client(AwsService.Sns.value, region_name=region_name)
        response = client.create_topic(Name=topic_name)
        return response['TopicArn']

    @staticmethod
    def delete_topic(topic_name: str, region_name: str, account_number: str):
        client = boto3.client(AwsService.Sns.value, region_name=region_name)
        topic_arn = "arn:aws:{}:{}:{}:{}".format(
            AwsService.Sns.value, region_name, account_number, topic_name)
        client.delete_topic(TopicArn=topic_arn)

    @staticmethod
    def get_content(file_bucket: str, file_key: str):
        return S3Helpers.get_binary_content(file_bucket, file_key).decode('utf-8')

    @staticmethod
    def get_binary_content(file_bucket: str, file_key: str):
        s3_handler = boto3.resource(AwsService.S3.value)
        obj = s3_handler.Object(file_bucket, file_key)
        return obj.get()['Body'].read()

    @staticmethod
    def get_version(file_bucket: str, file_key: str):
        client = boto3.client(AwsService.S3.value)
        response = client.get_object(Bucket=file_bucket, Key=file_key)
        return response['VersionId']

    @staticmethod
    def get_header(file_bucket: str, file_key: str):
        client = boto3.client(AwsService.S3.value)
        res = client.get_object(Bucket=file_bucket, Key=file_key)
        return res['Body'].read().decode('utf-8').split('\r\n')[0]
