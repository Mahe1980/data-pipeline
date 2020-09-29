from pytest import fixture, mark

from lambda_file_processing_trigger.src.event_processor.execution_context import ExecutionContext

event = {
    "AwsAccountNo": 123456,
    "Type": "UAT",
    "Records": [
        {
            "eventVersion": "2.1",
            "eventSource": "aws:s3",
            "awsRegion": "eu-west-1",
            "eventTime": "2018-12-05T13:29:30.914Z",
            "eventName": "ObjectCreated:Put",
            "userIdentity": {
                "principalId": "AWS:AROAJT2KVUZUXRHFNVBAC:IT-DPT-Team@icgam.com"
            },
            "requestParameters": {
                "sourceIPAddress": "52.178.223.233"
            },
            "responseElements": {
                "x-amz-request-id": "D795A252AD821DCC",
                "x-amz-id-2": "XuZApCE54Kj8XeAvgV2El1WH5NrKzPAZPLc0azAz73DFByYXxYJh4fXjkncfYMdInnirZPA6934="
            },
            "s3": {
                "s3SchemaVersion": "1.0",
                "configurationId": "tf-s3-lambda_file_processing_trigger-20181205131211186200000006",
                "bucket": {
                    "name": "icg-dl-raw-zone-1-primary",
                    "ownerIdentity": {
                        "principalId": "AC1GEWFOJIAW4"
                    },
                    "arn": "arn:aws:s3:::icg-dl-raw-zone-1-primary"
                },
                "object": {
                    "key": "drop/virtus/Dec_2018_Test.csv",
                    "size": 23927,
                    "eTag": "e373cd029520df51f5fa77198b611c7d",
                    "versionId": "B3MF9UXDgUWXW_uN4LalkPn3Brh5qdmu",
                    "sequencer": "005C07D2BAB32AFF83"
                }
            }
        }
    ]
}

test_bucket_extraction = [(event, "icg-dl-raw-zone-1-primary")]
test_object_key_extraction = [(event, "drop/virtus/Dec_2018_Test.csv")]
test_object_version_extraction = [(event, "B3MF9UXDgUWXW_uN4LalkPn3Brh5qdmu")]
test_processed_on_extraction = [(event, "2018-12-05T13:29:30.914Z")]

test_env_variables_success = [
    ({"business_process": "test_process_name",
      "pipeline_name": "test_pipeline_name",
      "business_email": "test_business_email@icgam.com",
      "support_email": "test_support_email@icgam.com",
      "target_bucket": "test_target_bucket",
      "error_topic": "error-notifications",
      "job_name": "tests-job",
      "log_level": "INFO",
      "aws_account_number": "123456"
      })
]


class HandlerContext:
    def __init__(self):
        self._aws_request_id = 'abc123456'

    @property
    def aws_request_id(self):
        return self._aws_request_id


@fixture
def handler_context():
    return HandlerContext


@fixture
def context():
    return ExecutionContext


def test_context_extracted(context, handler_context):
    context_handler = handler_context()
    lambda_context_obj = context(lambda_context=context_handler, event=event)
    assert lambda_context_obj.aws_request_id == "abc123456"


@mark.parametrize("s3_event, expected", test_bucket_extraction)
def test_bucket_id_extraction(s3_event, expected, context, handler_context):
    context_handler = handler_context()
    bucket_id = context(lambda_context=context_handler,
                        event=event).source_bucket_name
    assert bucket_id == expected


@mark.parametrize("s3_event, expected_key", test_object_key_extraction)
def test_object_data_extraction_key(s3_event, expected_key, context, handler_context):
    context_handler = handler_context()
    object_key = context(lambda_context=context_handler,
                         event=event).source_object_key
    assert object_key == expected_key


@mark.parametrize("s3_event, expected_version", test_object_version_extraction)
def test_object_data_extraction_version(s3_event, expected_version, context, handler_context):
    context_handler = handler_context()
    object_version = context(
        lambda_context=context_handler, event=event).source_object_version
    assert object_version == expected_version


@mark.parametrize("s3_event, expected", test_processed_on_extraction)
def test_processed_on_extraction(s3_event, expected, context, handler_context):
    context_handler = handler_context()
    processed_on_timestamp = context(
        lambda_context=context_handler, event=event).event_timestamp
    assert processed_on_timestamp == expected


@fixture(autouse=True)
def env_setup(monkeypatch):
    monkeypatch.setenv("BUSINESS_PROCESS", "test_process_name")
    monkeypatch.setenv("PIPELINE_NAME", "test_pipeline_name")
    monkeypatch.setenv("BUSINESS_EMAIL", "test_business_email@icgam.com")
    monkeypatch.setenv("SUPPORT_EMAIL", "test_support_email@icgam.com")
    monkeypatch.setenv("TARGET_BUCKET", "test_target_bucket")
    monkeypatch.setenv("ERROR_TOPIC", "error-notifications")
    monkeypatch.setenv("JOB_NAME", "tests-job")
    monkeypatch.setenv("LOG_LEVEL", "INFO")
    monkeypatch.setenv("AWS_ACCOUNT_NO", "123456")


@mark.parametrize("expected", test_env_variables_success)
def test_successful_access_to_env_vars(expected, context):
    env_obj = ExecutionContext(lambda_context=context, event=event)
    assert env_obj.business_process == expected["business_process"]
    assert env_obj.pipeline_name == expected["pipeline_name"]
    assert env_obj.business_email_to == expected["business_email"]
    assert env_obj.support_email_to == expected["support_email"]
    assert env_obj.target_bucket_name == expected["target_bucket"]
    assert env_obj.error_topic == expected["error_topic"]
    assert env_obj.job_name == expected["job_name"]
    assert env_obj.log_level == expected["log_level"]
    assert env_obj.aws_account_number == expected["aws_account_number"]
