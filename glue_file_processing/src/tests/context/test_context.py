from pytest import fixture, mark

from glue_file_processing.src.glue_file_processing.process_context import ProcessContext

test_context = [
    ({"correlation_id": 1, "file_key": "b/c.txt", "file_bucket": "a", "file_version_id": "1", "target_bucket": "tests",
      "processed_on": "2019-07-14T00:00", "business_process": "tests", "pipeline_name": "tests",
      "business_email": "tests@tests.com",
      "support_email": "supporttest@tests.com",
      "account_number": "123456789012",
      "error_details_crawler": "dev3-cfm-error-detailed-reporting",
      "error_summary_crawler": "dev3-cfm-error-summary-reporting"},
     {"correlation_id": 1, "file_key": "b/c.txt", "file_bucket": "a", "file_version_id": "1", "target_bucket": "tests",
      "processed_on": "2019-07-14T00:00", "business_process": "tests", "pipeline_name": "tests",
      "business_email": "tests@tests.com",
      "support_email": "supporttest@tests.com",
      "account_number": "123456789012",
      "error_details_crawler": "dev3-cfm-error-detailed-reporting",
      "error_summary_crawler": "dev3-cfm-error-summary-reporting"},
     True)
]


@fixture
def context():
    return ProcessContext


@mark.parametrize("test_input, expected, assertion", test_context)
def test_context_object_instantiation(context, test_input, expected, assertion):
    obj_context = context(**test_input)
    assert ({
        "correlation_id": obj_context.correlation_id,
        "file_key": obj_context.file_key,
        "file_bucket": obj_context.file_bucket,
        "file_version_id": obj_context.file_version_id,
        "target_bucket": obj_context.target_bucket,
        "processed_on": obj_context.processed_on,
        "business_process": obj_context.business_process,
        "pipeline_name": obj_context.pipeline_name,
        "business_email": obj_context.business_email,
        "support_email": obj_context.support_email,
        "account_number": obj_context.account_number,
        "error_details_crawler": obj_context.error_details_crawler,
        "error_summary_crawler": obj_context.error_summary_crawler
    } == expected) == assertion
