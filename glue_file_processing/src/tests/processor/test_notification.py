from core.aws import AwsRegion
from sns.topics import Topic
from moto import mock_sns, mock_s3
from pytest import mark

from glue_file_processing.src.glue_file_processing.boto_session import BotoSession
from glue_file_processing.src.glue_file_processing.pipeline_enum import SnsStatus
from glue_file_processing.src.glue_file_processing.process_steps import ProcessSteps

test_notification_success = [
    ({"correlation_id": '1', "file_key": "a/b/c.txt", "file_bucket": "tests", "target_bucket": "tests",
      "file_version_id": "1", "processed_on": "2019-07-03T14:00", "business_process": "tests", "pipeline_name": "tests",
      "business_email": "tests@tests.com", "support_email": "tests@tests.com", "account_number": "123456789012",
      "error_details_crawler": "dev3-cfm-error-detailed-reporting",
      "error_summary_crawler": "dev3-cfm-error-summary-reporting"},
     "Glue Job Successful", "Email Content",
     'AsOfDate|MasterBusinessLineId|MasterBusinessLineName|MasterFundGroupId|MasterFundGroupName|'
     'MasterFundGroupFriendlyName|MasterFundGroupStatusId|PortfolioId|PortfolioName|FundGroupCurrency|'
     'MasterCurrency\n"2019-07-04 00:00"|1|tests|1|tests|tests|1|1|tests|tests|tests')
]

test_notification_failure = [
    ({"correlation_id": '1', "file_key": "a/b/c.txt", "file_bucket": "tests", "target_bucket": "tests",
      "file_version_id": "1", "processed_on": "2019-07-03T14:00", "business_process": "tests", "pipeline_name": "tests",
      "business_email": "tests@tests.com", "support_email": "tests@tests.com", "account_number": "123456789012",
      "error_details_crawler": "dev3-cfm-error-detailed-reporting",
      "error_summary_crawler": "dev3-cfm-error-summary-reporting"},
     "Glue Job Failed", "Email Content",
     'AsOfDate|MasterBusinessLineId|MasterBusinessLineName|MasterFundGroupId|MasterFundGroupName|'
     'MasterFundGroupFriendlyName|MasterFundGroupStatusId|PortfolioId|PortfolioName|FundGroupCurrency|'
     'MasterCurrency\n"2019-07-04T00:00"|1|tests|1|tests|tests|1|1|tests|tests|tests')
]


@mock_sns
@mock_s3
class TestNotification:
    @mark.parametrize("context_input, subject, msg, file_data", test_notification_success)
    def test_notification_success(self, context, s3_helper, context_input, subject, msg, file_data):
        s3_helper.create_bucket(file_bucket=context_input["file_bucket"])
        s3_helper.create_file_key(file_bucket=context_input["file_bucket"], file_key=context_input["file_key"],
                                  file_data=bytes(file_data, encoding='utf8'))
        s3_helper.create_topic(Topic.Success.value, AwsRegion.EUIreland.value)
        process_context = context(**context_input)
        ProcessSteps(BotoSession().get_session(AwsRegion.EUIreland)).send_notification(context=process_context,
                                                                                       topic=Topic.Success,
                                                                                       region_name=AwsRegion.EUIreland,
                                                                                       status=SnsStatus.Success,
                                                                                       subject=subject, msg=msg,
                                                                                       email_to=context_input[
                                                                                           "business_email"])

    @mark.parametrize("context_input, subject, msg, file_data", test_notification_failure)
    def test_notification_failure(self, context, s3_helper, context_input, subject, msg, file_data):
        s3_helper.create_bucket(file_bucket=context_input["file_bucket"])
        s3_helper.create_file_key(file_bucket=context_input["file_bucket"], file_key=context_input["file_key"],
                                  file_data=bytes(file_data, encoding='utf8'))
        s3_helper.create_topic(Topic.Error.value, AwsRegion.EUIreland.value)
        process_context = context(**context_input)
        ProcessSteps(BotoSession().get_session(AwsRegion.EUIreland)).send_notification(context=process_context,
                                                                                       topic=Topic.Error,
                                                                                       region_name=AwsRegion.EUIreland,
                                                                                       status=SnsStatus.Failed,
                                                                                       subject=subject, msg=msg,
                                                                                       email_to=context_input[
                                                                                           "support_email"])
