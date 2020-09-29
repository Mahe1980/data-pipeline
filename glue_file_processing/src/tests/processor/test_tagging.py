import pytest
from moto import mock_s3
from core.aws import AwsRegion
from tagger.exceptions import TagNotAllowedError

from glue_file_processing.src.glue_file_processing.boto_session import BotoSession
from glue_file_processing.src.glue_file_processing.process_steps import ProcessSteps

test_tag = [
    ({"file_key": "a/b/c.txt", "file_bucket": "tests", "file_version_id": "1"},
     {"hash": "sample-hash", 'correlation_id': '1', 'status': 'processed'}, True),
    ({"file_key": "a/b/d.txt", "file_bucket": "tests", "file_version_id": "1"},
     {"hash": "sample-hash", 'overall_quality_score': '1', 'status': 'failed'}, True)
]

test_incorrect_tag = [
    ({"file_key": "a/b/c.txt", "file_bucket": "tests", "file_version_id": "1"},
     {"hash": "tests-hash", 'correlation_id': '3', 'status': 'processed'}, False)
]

test_tag_not_allowed = [
    ({"file_key": "a/b/e.txt", "file_bucket": "tests", "file_version_id": "1"},
     {"not_allowed_key": "some_value"})
]

test_append_tags = [({"file_key": "a/b/c.txt", "file_bucket": "tests", "file_version_id": "1"},
                     {"hash": "sample-hash", 'correlation_id': '1', 'status': 'processed'}, True)]


@mock_s3
class TestTagging:
    @pytest.mark.parametrize("input_data, tags, assertion", test_tag)
    def test_tagging(self, s3_helper, input_data, tags, assertion):
        s3_helper.create_bucket(file_bucket=input_data["file_bucket"])
        s3_helper.create_file_key(file_bucket=input_data["file_bucket"], file_key=input_data["file_key"],
                                  file_data="this is a tests file")
        ProcessSteps(BotoSession().get_session(AwsRegion.EUIreland)).tag_file(tags=tags,
                                                                              file_bucket=input_data["file_bucket"],
                                                                              file_key=input_data["file_key"],
                                                                              file_version_id=input_data[
                                                                                  "file_version_id"])
        actual_tags = s3_helper.get_tagging(file_bucket=input_data["file_bucket"],
                                            file_key=input_data["file_key"])
        assert (actual_tags == tags) == assertion

    @pytest.mark.parametrize("input_data, tags, assertion", test_incorrect_tag)
    def test_incorrect_tag(self, s3_helper, input_data, tags, assertion):
        s3_helper.create_bucket(file_bucket=input_data["file_bucket"])
        s3_helper.create_file_key(file_bucket=input_data["file_bucket"], file_key=input_data["file_key"],
                                  file_data="this is a tests file")
        ProcessSteps(BotoSession().get_session(AwsRegion.EUIreland)).tag_file(tags=tags,
                                                                              file_bucket=input_data["file_bucket"],
                                                                              file_key=input_data["file_key"],
                                                                              file_version_id=input_data[
                                                                                  "file_version_id"])
        actual_tags = s3_helper.get_tagging(file_bucket=input_data["file_bucket"],
                                            file_key=input_data["file_key"])
        tags['hash'] = 'hash-tests'
        assert (actual_tags == tags) == assertion

    @pytest.mark.parametrize("input_data, tags", test_tag_not_allowed)
    def test_tag_not_allowed(self, s3_helper, input_data, tags):
        s3_helper.create_bucket(file_bucket=input_data["file_bucket"])
        s3_helper.create_file_key(file_bucket=input_data["file_bucket"], file_key=input_data["file_key"],
                                  file_data="this is a tests file")
        with pytest.raises(TagNotAllowedError):
            ProcessSteps(BotoSession().get_session(AwsRegion.EUIreland)).tag_file(tags=tags,
                                                                                  file_bucket=input_data["file_bucket"],
                                                                                  file_key=input_data["file_key"],
                                                                                  file_version_id=input_data[
                                                                                      "file_version_id"]
                                                                                  )

    @pytest.mark.parametrize("input_data, tags, assertion", test_append_tags)
    def test_append_tags(self, s3_helper, input_data, tags, assertion):
        s3_helper.create_bucket(file_bucket=input_data["file_bucket"])
        s3_helper.create_file_key(file_bucket=input_data["file_bucket"], file_key=input_data["file_key"],
                                  file_data="this is a tests file")
        ProcessSteps(BotoSession().get_session(AwsRegion.EUIreland)).tag_file(tags=tags,
                                                                              file_bucket=input_data["file_bucket"],
                                                                              file_key=input_data["file_key"],
                                                                              file_version_id=input_data[
                                                                                  "file_version_id"])
        ProcessSteps(BotoSession().get_session(AwsRegion.EUIreland)).tag_file(tags={'overall_quality_score': 0.5},
                                                                              file_bucket=input_data["file_bucket"],
                                                                              file_key=input_data["file_key"],
                                                                              file_version_id=input_data[
                                                                                  "file_version_id"])
        actual_tags = s3_helper.get_tagging(file_bucket=input_data["file_bucket"],
                                            file_key=input_data["file_key"])
        tags['overall_quality_score'] = '0.5'
        assert (actual_tags == tags) == assertion
