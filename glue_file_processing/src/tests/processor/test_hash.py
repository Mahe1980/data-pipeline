from core.aws import AwsRegion
from moto import mock_s3
from pytest import mark

from glue_file_processing.src.glue_file_processing.boto_session import BotoSession
from glue_file_processing.src.glue_file_processing.process_steps import ProcessSteps

test_hash = [
    ({"file_key": "a/b/c.txt", "file_bucket": "tests"},
     "d1fa398af76fd27e9a25965e066f9169974b8ada8bcc02559323ac0b8e3bf61f", True),
    ({"file_key": "a/b/c.txt", "file_bucket": "tests"},
     "this-is-incorrect-hash", False)
]


@mark.parametrize("test_input, expected, assertion", test_hash)
@mock_s3
class TestCalculateHash:
    def test_calculate_hash(self, s3_helper, test_input, expected, assertion):
        s3_helper.create_bucket(file_bucket=test_input["file_bucket"])
        s3_helper.create_file_key(file_bucket=test_input["file_bucket"], file_key=test_input["file_key"],
                                  file_data="this is a tests file")
        actual_hash = ProcessSteps(BotoSession().get_session(AwsRegion.EUIreland)).calculate_hash(
            file_bucket=test_input["file_bucket"], file_key=test_input["file_key"])
        assert (actual_hash == expected) == assertion
