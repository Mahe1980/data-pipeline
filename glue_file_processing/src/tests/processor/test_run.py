import re
import uuid
from datetime import datetime
from unittest.mock import patch

from core.aws import AwsRegion
from data_quality.data_quality_const import QualityRuleConst
from moto import mock_s3, mock_sns, mock_glue
from pytest import mark, raises, fixture
from sns.topics import Topic

from glue_file_processing.src.glue_file_processing.pipeline_configuration import CONFIGURATION
from glue_file_processing.src.glue_file_processing.processor import Processor

# assume for testing that the last parameter is the column list
source_data_columns = CONFIGURATION['schema']
source_data_columns = [x["name"] for x in source_data_columns if not x.get("is_calculated")]
source_data_columns_missing_cols = source_data_columns[:-4]
date_format = CONFIGURATION['file_name_timestamp']
delimiter = CONFIGURATION['delimiter']

test_data_success = [
    ({"correlation_id": '1', "file_key": "BusinessArea/BusinessProcess/DataSource/Automated/test1_20191003103000.txt",
      "file_bucket": "raw_bucket", "target_bucket": "curated_bucket",
      "processed_on": "2019-07-30T10:00:54.129Z", "business_process": "BusinessArea", "pipeline_name": "tests",
      "business_email": "tests@tests.com", "support_email": "tests@tests.com", "account_number": "123456789012",
      "file_version_id": "",
      "error_details_crawler": "dev3-cfm-error-detailed-reporting",
      "error_summary_crawler": "dev3-cfm-error-summary-reporting"}, source_data_columns)

]

test_run_missing_columns_failure = [
    ({"correlation_id": '2', "file_key": "BusinessArea/BusinessProcess/DataSource/Automated/test2_20191003103100.txt",
      "file_bucket": "raw1_bucket", "target_bucket": "curated_bucket",
      "processed_on": "2019-07-30T10:00:54.129Z", "business_process": "BusinessArea", "pipeline_name": "tests",
      "business_email": "tests@tests.com", "support_email": "tests@tests.com", "account_number": "123456789012",
      "file_version_id": "",
      "error_details_crawler": "dev3-cfm-error-detailed-reporting",
      "error_summary_crawler": "dev3-cfm-error-summary-reporting"}, source_data_columns_missing_cols
     )
]

test_data_invalid_processed_on_error = [
    ({"correlation_id": '3', "file_key": "BusinessArea/BusinessProcess/DataSource/Automated/test3_20191003103000.txt",
      "file_bucket": "raw_bucket", "target_bucket": "curated_bucket",
      "processed_on": "2019-07-30T00:00", "business_process": "BusinessArea", "pipeline_name": "tests",
      "business_email": "tests@tests.com", "support_email": "tests@tests.com", "account_number": "0000000000",
      "file_version_id": "",
      "error_details_crawler": "dev3-cfm-error-detailed-reporting",
      "error_summary_crawler": "dev3-cfm-error-summary-reporting"}, source_data_columns
     )
]


class InitialSetUp:
    @staticmethod
    def create_bucket_key(s3_helper, file_bucket, file_key, file_data, target_bucket):
        s3_helper.create_bucket(file_bucket=file_bucket)
        s3_helper.create_bucket(file_bucket=target_bucket)
        s3_helper.create_file_key(file_bucket=file_bucket, file_key=file_key,
                                  file_data=bytes('{}'.format(delimiter).join(file_data), encoding='utf8'))


class TargetKey:
    @staticmethod
    def get_target_key(file_key):
        root_folder = file_key[:file_key.find("Automated") - 1]
        file_name = file_key.split('/')[-1]
        match = re.search(r'\d{4}\d{2}\d{2}\d{2}\d{2}\d{2}', file_name)
        datetime.strptime(match.group(), date_format)
        as_of_date = match.group()
        return f"{root_folder}/{as_of_date[:4]}/{as_of_date[:4]}{as_of_date[4:6]}/{file_name}"


@fixture
def initialise():
    return InitialSetUp


@fixture
def target_key():
    return TargetKey


@mock_glue
@mock_sns
@mock_s3
class TestRun:
    @mark.parametrize("context_input, file_data", test_data_success)
    def test_run_success(self, context, initialise, target_key, s3_helper, context_input, file_data):
        initialise.create_bucket_key(s3_helper, context_input["file_bucket"], context_input["file_key"], file_data,
                                     context_input["target_bucket"])
        context_input['file_version_id'] = s3_helper.get_version(file_bucket=context_input["file_bucket"],
                                                                 file_key=context_input["file_key"])
        process_context = context(**context_input)

        with patch(
                'glue_file_processing.src.glue_file_processing.pipeline_catalog.DataCatalogManager') as mock_catalog_manager:
            instance = mock_catalog_manager.return_value
            instance.is_table_needing_updating_schema.return_value = False
            Processor().run(context=process_context)

        key_target = target_key.get_target_key(context_input["file_key"])
        assert s3_helper.s3_key_exists(
            context_input["target_bucket"], key_target) is True, "The file must be copied to the target location"
        header = s3_helper.get_header(
            context_input["target_bucket"], key_target)
        expected_header = '{}'.format(delimiter).join([s for s in file_data])
        tags = s3_helper.get_tagging(context_input["target_bucket"], key_target)
        score_summary = f'{QualityRuleConst.BLANK}=1.0:' \
            f'{QualityRuleConst.COLUMN_COUNT}=1.0:' \
            f'{QualityRuleConst.COLUMN_NAMES}=1.0:' \
            f'{QualityRuleConst.OUTLIERS}=1.0:' \
            f'{QualityRuleConst.DATA_TYPES}=1.0:' \
            f'{QualityRuleConst.CONDITION_CHECK}=1.0:' \
            f'{QualityRuleConst.PRIMARY_ID}=1.0:' \
            f'{QualityRuleConst.DUPLICATES}=1.0'
        status_summary = f'{QualityRuleConst.BLANK}=P:' \
            f'{QualityRuleConst.COLUMN_COUNT}=P:' \
            f'{QualityRuleConst.COLUMN_NAMES}=P:' \
            f'{QualityRuleConst.OUTLIERS}=P:' \
            f'{QualityRuleConst.DATA_TYPES}=P:' \
            f'{QualityRuleConst.CONDITION_CHECK}=P:' \
            f'{QualityRuleConst.PRIMARY_ID}=P:' \
            f'{QualityRuleConst.DUPLICATES}=P'
        expected = {
            'status': 'processed', 'correlation_id': '1', 'overall_quality_score': '1.0',
            'hash': '0399c05ec63909caf5295ba4e3e254db37d694b35e20bca9a7e34b7222e6c59f',
            'quality_score_summary': score_summary,
            'quality_status_summary': status_summary
        }

        assert header == expected_header
        assert expected == tags

    @mark.parametrize("context_input, file_data", test_run_missing_columns_failure)
    def test_run_failure(self, context, initialise, target_key, s3_helper, context_input, file_data):
        initialise.create_bucket_key(s3_helper, context_input["file_bucket"], context_input["file_key"], file_data,
                                     context_input["target_bucket"])

        context_input['file_version_id'] = s3_helper.get_version(file_bucket=context_input["file_bucket"],
                                                                 file_key=context_input["file_key"])
        topic_arn = s3_helper.create_topic(Topic.Error.value, AwsRegion.EUIreland.value)
        context_input['account_number'] = topic_arn.split(':')[-2]

        process_context = context(**context_input)

        with patch(
                'glue_file_processing.src.glue_file_processing.pipeline_catalog.DataCatalogManager') as mock_catalog_manager:
            instance = mock_catalog_manager.return_value
            instance.is_table_needing_updating_schema.return_value = False
            Processor().run(context=process_context)

        key_target = target_key.get_target_key(context_input["file_key"])
        assert s3_helper.s3_key_exists(
            context_input["target_bucket"], key_target) is False

    @mark.parametrize("context_input, file_data", test_data_invalid_processed_on_error)
    def test_run_invalid_processed_on_error(self, context, initialise, s3_helper, context_input, file_data):
        initialise.create_bucket_key(s3_helper, context_input["file_bucket"], context_input["file_key"], file_data,
                                     context_input["target_bucket"])
        context_input['file_version_id'] = s3_helper.get_version(file_bucket=context_input["file_bucket"],
                                                                 file_key=context_input["file_key"])

        s3_helper.create_topic(Topic.Error.value, AwsRegion.EUIreland.value)
        process_context = context(**context_input)
        with raises(Exception):
            Processor().run(context=process_context)


test_data = [
    ({"correlation_id": 'testrun:' + str(uuid.uuid4()),
      "file_key": "CFM/Everest/Holdings/Automated/Holdings20191121233126.csv",
      "file_bucket": "dev3-icg-dl-raw-zone-primary", "target_bucket": "dev3-icg-dl-curated-zone-primary",
      "processed_on": "2019-12-18T10:00:54.129Z", "business_process": "Everest",
      "pipeline_name": "cfm_everest_holdings",
      "business_email": "tests@tests.com", "support_email": "test@test.com",
      "account_number": "249637478524",
      "file_version_id": "VgJXkFpzPFPV7RuOKzvwVxP2j8T5S71b",
      "error_details_crawler": "dev3-cfm-error-detailed-reporting",
      "error_summary_crawler": "dev3-cfm-error-summary-reporting"}, source_data_columns),
    ({"correlation_id": 'testrun:' + str(uuid.uuid4()),
      "file_key": "CFM/Everest/Holdings/Automated/Holdings20200401000000.csv",
      "file_bucket": "dev3-icg-dl-raw-zone-primary", "target_bucket": "dev3-icg-dl-curated-zone-primary",
      "processed_on": "2019-12-18T10:00:54.129Z", "business_process": "Everest",
      "pipeline_name": "cfm_everest_holdings",
      "business_email": "tests@tests.com", "support_email": "test@test.com",
      "account_number": "249637478524",
      "file_version_id": "U.pb0Nev.b4EeWkhDSsjFwxiwl5VeC7C",
      "error_details_crawler": "dev3-cfm-error-detailed-reporting",
      "error_summary_crawler": "dev3-cfm-error-summary-reporting"}, source_data_columns)
]


@mark.parametrize("context_input, file_data", test_data)
@mark.skip(reason="This runs on actual aws so skip this in teamcity.")
def test_run_success_on_aws(context, context_input, file_data):
    process_context = context(**context_input)
    Processor().run(context=process_context)
