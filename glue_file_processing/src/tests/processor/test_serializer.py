# pylint:disable=redefined-outer-name
from glue_file_processing.src.glue_file_processing.pipeline_parquet import PipelineParquet
import pandas as pd

from core.aws import AwsRegion
from file_operations.context import Context
from moto import mock_s3
from pytest import fixture

from glue_file_processing.src.glue_file_processing.boto_session import BotoSession
from glue_file_processing.src.glue_file_processing.config import Config

CONFIG = Config({'delimiter': ',', 'file_name_timestamp': '%Y%m%d%H%M%S',
                 "schema": [
                     {"name": "asofdate", "type": "DATE", "is_calculated": "true"},
                     {"name": "OrderDate", "type": "TIMESTAMP"},
                     {"name": "TradeDate", "type": "DATE"},
                     {"name": "TranType", "type": "STRING"},
                     {"name": "Quantity", "type": "DECIMAL"},
                     {"name": "row_index", "type": "INT", "is_calculated": "true"},
                     {"name": "correlation_id", "type": "STRING", "is_calculated": "true"},
                     {"name": "confidence_level", "type": "DECIMAL(3,2)", "is_calculated": "true"},

                 ]
                 })


@fixture
def context():
    return Context(file_bucket="test", file_key="cfm/bla/bla/", file_version_id="1",
                   target_bucket="test", target_key="cfm/bla/bla/AutomatedStatus/Processed/")


@mock_s3
def test_parquet_writer(s3_helper, context):
    s3_helper.create_bucket(file_bucket=context.target_bucket)

    s3_helper.create_file_key(file_bucket=context.target_bucket,
                              file_key=f"{context.target_key}text_20191101125900.csv",
                              file_data=bytes(
                                  'OrderDate,TradeDate,TranType,Quantity\n'
                                  '"2019-11-10T17:07:01","2019-11-10","Test","34.3445566"',
                                  encoding='utf8'))

    pipeline_parquet = PipelineParquet(BotoSession().get_session(AwsRegion.EUIreland))
    pipeline_parquet.create_parquet(target_bucket=context.target_bucket,
                                    target_key=f"{context.target_key}text_20191101125900.csv",
                                    config=CONFIG,
                                    error_indexes=pd.DataFrame(columns=['row_index']),
                                    correlation_id='1')
    assert s3_helper.s3_key_exists(file_bucket='test',
                                   file_key='cfm/bla/bla/parquet/year=2019/year_month=201911/year_month_day=20191101/'
                                            'text_20191101125900.parquet') is True


@mock_s3
def test_parquet_key(context):
    pipeline_parquet = PipelineParquet(BotoSession().get_session(AwsRegion.EUIreland))

    parquet_key = pipeline_parquet.get_parquet_key(
        target_key=f"{context.target_key}text_20191101125900.csv",
        fmt='%Y%m%d%H%M%S'
    )

    expected_parquet_key = ('cfm/bla/bla/parquet/year=2019/year_month=201911'
                            '/year_month_day=20191101/text_20191101125900.parquet')

    assert parquet_key == expected_parquet_key
