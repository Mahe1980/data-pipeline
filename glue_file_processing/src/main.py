import sys
from collections import namedtuple

from awsglue.utils import getResolvedOptions
from glue_file_processing.process_context import ProcessContext
from glue_file_processing.processor import Processor
from pip._internal.operations.freeze import freeze


def get_arguments():
    expected_args = ["correlation_id", "file_bucket", "file_key", "file_version_id", "target_bucket", "processed_on",
                     "business_process", "pipeline_name", "business_email", "support_email", "account_number",
                     "error_details_crawler", "error_summary_crawler"]
    args = getResolvedOptions(sys.argv, expected_args)

    ContextVariables = namedtuple("ContextVariables", expected_args)
    return ContextVariables(**args)


def main():
    for _package in freeze(local_only=True):
        print(_package)

    context_variables = get_arguments()
    context = ProcessContext(**context_variables._asdict())
    Processor().run(context)


if __name__ == '__main__':
    main()
