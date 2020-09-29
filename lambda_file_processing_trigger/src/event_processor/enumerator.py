from enum import Enum


class LogEnum(Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class EnvironmentVariables(Enum):
    BusinessProcess = "BUSINESS_PROCESS"
    PipelineName = "PIPELINE_NAME"
    BusinessEmail = "BUSINESS_EMAIL"
    SupportEmail = "SUPPORT_EMAIL"
    TargetBucket = "TARGET_BUCKET"
    ErrorTopic = "ERROR_TOPIC"
    JobName = "JOB_NAME"
    LogLevel = "LOG_LEVEL"
    AwsAccountNo = 'AWS_ACCOUNT_NO'
    ErrorDetailsCrawler = 'ERROR_DETAILS_CRAWLER_NAME'
    ErrorSummaryCrawler = 'ERROR_SUMMARY_CRAWLER_NAME'


class Event(Enum):
    Records = 'Records'
    Bucket = 'bucket'
    Name = 'name'
    Object = 'object'
    Key = 'key'
    VersionId = 'versionId'
    EventTime = 'eventTime'
