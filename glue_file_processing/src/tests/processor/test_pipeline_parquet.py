import copy
import datetime
import decimal
from contextlib import contextmanager
from typing import List

import pandas as pd
import pyarrow
from core.aws import AwsRegion
from gluecatalog.sanitize import sanitize_column_name
from moto import mock_s3, mock_sns, mock_glue
from pytest import fail
from pytest import mark, fixture, raises

from glue_file_processing.src.glue_file_processing.boto_session import BotoSession
from glue_file_processing.src.glue_file_processing.logger import get_logger
from glue_file_processing.src.glue_file_processing.pipeline_configuration import CONFIGURATION
from glue_file_processing.src.glue_file_processing.pipeline_configuration import ConfigurationException
from glue_file_processing.src.glue_file_processing.pipeline_parquet import PipelineParquet
from glue_file_processing.src.glue_file_processing.config import Config


@contextmanager
def success():
    yield


class InitialSetUp:
    @staticmethod
    def create_bucket_key(s3_helper, file_bucket, file_key, file_data, target_bucket):
        s3_helper.create_bucket(file_bucket=file_bucket)
        s3_helper.create_bucket(file_bucket=target_bucket)
        s3_helper.create_file_key(file_bucket=file_bucket, file_key=file_key,
                                  file_data=bytes('{}'.format(delimiter).join(file_data), encoding='utf8'))
        s3_helper.create_file_key(file_bucket=target_bucket, file_key=file_key,
                                  file_data=bytes('{}'.format(delimiter).join(file_data), encoding='utf8'))


logger = get_logger()
test_case_implemented_calculated_config = copy.deepcopy(CONFIGURATION)
delimiter = CONFIGURATION['delimiter']
all_schema_columns = CONFIGURATION['schema']
source_data_columns: List = [x["name"] for x in all_schema_columns if not x.get("is_calculated")]
expected_parquet_column_names = [sanitize_column_name(x["name"]) if CONFIGURATION.get('sanitize_columns') else x["name"]
                                 for x in all_schema_columns]


def config_new_source_column_appended():
    config = copy.deepcopy(CONFIGURATION)
    schema = config['schema']
    schema.append({"name": "new_column", "type": "INT"})
    config['schema'] = schema
    return config


def config_new_calculated_column_appended():
    config = copy.deepcopy(CONFIGURATION)
    schema = config['schema']
    schema.append({"name": "new_column_2", "type": "INT", "is_calculated": "true"})
    config['schema'] = schema
    return config


source_with_new_column = source_data_columns + ['new_column']
expected_parquet_column_names_with_new = expected_parquet_column_names + ['new_column']

test_data = [(
    "test case with implemented calculated columns appended",
    success(),
    "raw_zone1",
    "BusinessArea/BusinessProcess/DataSource/Automated/test1_20191003103000.txt",
    "curated_zone",
    "BusinessArea/BusinessProcess/DataSource/Automated/test1_20191003103000.txt",
    source_data_columns,
    expected_parquet_column_names,
    test_case_implemented_calculated_config,
    "BusinessArea/BusinessProcess/DataSource/parquet/year=2019/year_month=201910/year_month_day=20191003/"
    "test1_20191003103000.parquet"
),
    ("test case with new source column appended",
     success(),
     "raw_zone2",
     "BusinessArea/BusinessProcess/DataSource/Automated/test2_20191003103000.txt",
     "curated_zone2",
     "BusinessArea/BusinessProcess/DataSource/Automated/test2_20191003103000.txt",
     source_with_new_column,
     expected_parquet_column_names_with_new,
     config_new_source_column_appended(),
     "BusinessArea/BusinessProcess/DataSource/parquet/year=2019/year_month=201910/year_month_day=20191003/"
     "test2_20191003103000.parquet"
     ),
    ("test case with new calculated column not implemented",
     raises(KeyError),
     "raw_zone3",
     "BusinessArea/BusinessProcess/DataSource/Automated/test3_20191003103000.txt",
     "curated_zone3",
     "BusinessArea/BusinessProcess/DataSource/Automated/test3_20191003103000.txt",
     source_data_columns,
     expected_parquet_column_names_with_new,
     config_new_calculated_column_appended(),
     "no-key"
     )
]


@fixture
def initialise():
    return InitialSetUp


@mock_glue
@mock_sns
@mock_s3
class TestPipelineParquet:
    pipeline_parquet = PipelineParquet(BotoSession().get_session(AwsRegion.EUIreland))

    @mark.parametrize(
        "test_case, test_result, source_bucket, source_key, target_bucket, target_key, source_data_columns, "
        "expected_parquet_column_names, config, expected_key",
        test_data)
    def test_create_parquet_2(self, initialise, s3_helper, test_case, test_result, source_bucket, source_key,
                              target_bucket, target_key, source_data_columns, expected_parquet_column_names, config,
                              expected_key):
        error_indexes = pd.DataFrame(columns=['row_index'])
        correlation_id = 'xyz-123'
        initialise.create_bucket_key(s3_helper, source_bucket, source_key, source_data_columns, target_bucket)
        logger.info(f"Running test case {test_case}")
        with test_result:
            parquet_key = self.pipeline_parquet.create_parquet(target_bucket, target_key, Config(config), error_indexes,
                                                               correlation_id)
            assert parquet_key == expected_key
            file_content = pyarrow.py_buffer(s3_helper.get_binary_content(target_bucket, expected_key))
            table = pyarrow.parquet.read_table(file_content)
            df = table.to_pandas(ignore_metadata=True)
            assert list(df.columns.values) == expected_parquet_column_names
