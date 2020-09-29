from pytest import fixture

from glue_file_processing.src.glue_file_processing.process_context import ProcessContext
from .processor.s3_helper import S3Helpers


@fixture
def s3_helper():
    return S3Helpers


@fixture
def context():
    return ProcessContext
