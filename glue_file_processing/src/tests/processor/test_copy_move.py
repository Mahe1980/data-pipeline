from core.aws import AwsRegion
from file_operations.context import Context as FileOperationsContext
from moto import mock_s3
from pytest import mark

from glue_file_processing.src.glue_file_processing.boto_session import BotoSession
from glue_file_processing.src.glue_file_processing.process_steps import ProcessSteps

test_copy = [
    ({"file_key": "a/b/c.txt", "file_bucket": "tests",
      "target_key": "d/e/f.txt", "target_bucket": "tests"}, True)
]

test_move = [
    ({"file_key": "a/b/d.txt", "file_bucket": "tests",
      "target_key": "d/e/f.txt", "target_bucket": "tests"}, True)
]


@mock_s3
class TestCopyMove:
    @mark.parametrize("input_data, expected", test_copy)
    def test_copy(self, s3_helper, input_data, expected):
        s3_helper.create_bucket(file_bucket=input_data["file_bucket"])
        s3_helper.create_file_key(file_bucket=input_data["file_bucket"], file_key=input_data["file_key"],
                                  file_data="this is a tests file")
        file_version_id = s3_helper.get_version(
            file_bucket=input_data["file_bucket"], file_key=input_data["file_key"])
        file_operation_context = FileOperationsContext(file_bucket=input_data["file_bucket"],
                                                       file_key=input_data["file_key"],
                                                       file_version_id=file_version_id,
                                                       target_bucket=input_data["target_bucket"],
                                                       target_key=input_data["target_key"]
                                                       )
        ProcessSteps(BotoSession().get_session(AwsRegion.EUIreland)
                     ).copy_file(file_operation_context)
        is_exists = s3_helper.s3_key_exists(file_bucket=input_data["file_bucket"],
                                            file_key=input_data["target_key"])
        assert is_exists == expected
        is_exists = s3_helper.s3_key_exists(file_bucket=input_data["file_bucket"],
                                            file_key=input_data["file_key"])
        assert is_exists == expected

    @mark.parametrize("input_data, expected", test_move)
    def test_move(self, s3_helper, input_data, expected):
        s3_helper.create_bucket(file_bucket=input_data["file_bucket"])
        s3_helper.create_file_key(file_bucket=input_data["file_bucket"], file_key=input_data["file_key"],
                                  file_data="this is a tests file")
        file_version_id = s3_helper.get_version(
            file_bucket=input_data["file_bucket"], file_key=input_data["file_key"])
        file_operation_context = FileOperationsContext(file_bucket=input_data["file_bucket"],
                                                       file_key=input_data["file_key"],
                                                       file_version_id=file_version_id,
                                                       target_bucket=input_data["target_bucket"],
                                                       target_key=input_data["target_key"]
                                                       )
        ProcessSteps(BotoSession().get_session(AwsRegion.EUIreland)
                     ).move_file(file_operation_context)
        is_exists = s3_helper.s3_key_exists(file_bucket=input_data["file_bucket"],
                                            file_key=input_data["target_key"])
        assert is_exists == expected
        is_exists = s3_helper.s3_key_exists(file_bucket=input_data["file_bucket"],
                                            file_key=input_data["file_key"])
        assert not is_exists == expected
