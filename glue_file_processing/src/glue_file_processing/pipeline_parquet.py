import typing

import pandas as pd
from boto3 import Session

from file_operations.context import Context as FileOperationsContext
from file_operations.file_operations import S3FileOperations
from gluecatalog.model import Column
from gluecatalog.sanitize import sanitize_data_frame_col_names
from serializer.parquet_util import ParquetUtil
from serializer.parquet_writer import ParquetWriter

from .logger import get_logger
from .process_steps import ProcessSteps
from .transform.extend import with_as_of_date, with_error_columns
from .config import Config
from .util.path import get_filename_timestamp, get_root_folder, get_file_name


LOGGER = get_logger()


class PipelineParquet:
    date_col_name = 'asofdate'
    confidence_level_col_name = 'confidence_level'
    row_index_col_name = 'row_index'
    correlation_id_col_name = 'correlation_id'

    def __init__(self, session: Session):
        self._session = session
        self._process_step = ProcessSteps(self._session)

    def get_parquet_key(self, target_key: str, fmt: str) -> str:
        root_folder = get_root_folder(target_key)
        file_name = get_file_name(target_key, include_extension=False)
        year, year_month, year_month_day = self.get_partition_values(file_name, fmt)
        parquet_key = (f"{root_folder}/parquet/"
                       f"year={year}/year_month={year_month}/year_month_day={year_month_day}/"
                       f"{file_name}.parquet")

        LOGGER.info(f"Calculated parquet key '{parquet_key}'")
        return parquet_key

    def create_parquet(
            self,
            target_bucket: str,
            target_key: str,
            config: Config,
            error_indexes: pd.DataFrame,
            correlation_id: str) -> str:
        schema_columns = config.catalog_columns(include_calculated_cols=False)
        parquet_key = self.get_parquet_key(target_key, config.filename_timestamp_fmt)
        parquet_writer = ParquetWriter(self._session)
        data_frame = self._get_dataframe(target_bucket, target_key, schema_columns, config.delimiter,
                                         config.sanitize_columns)

        timestamp = get_filename_timestamp(target_key, config.filename_timestamp_fmt)
        df_with_as_of_date = with_as_of_date(data_frame, config.as_of_date, timestamp)
        df_with_error_columns = with_error_columns(df_with_as_of_date, error_indexes, correlation_id, config.schema)

        all_schema_columns = config.catalog_columns(include_calculated_cols=True)
        schema = ParquetUtil.create_schema(all_schema_columns)
        parquet_writer.write_parquet_file(df_with_error_columns, schema, f"s3://{target_bucket}/{parquet_key}")
        LOGGER.info(f"Created parquet file '{parquet_key}'")
        return parquet_key

    def _get_dataframe(self, target_bucket: str, target_key: str, cols_flattened: typing.List[Column], delimiter: str,
                       sanitize_columns: bool):
        data_frame = self.__create_dataframe(target_bucket, target_key, delimiter)
        data_frame = sanitize_data_frame_col_names(data_frame) if sanitize_columns else data_frame
        return ParquetUtil.convert_data_frame(data_frame, cols_flattened)

    def __create_dataframe(self, target_bucket: str, target_key: str, delimiter: str):
        target_file_context = FileOperationsContext(file_key=target_key, file_bucket=target_bucket)
        file_ops = S3FileOperations(session=self._session)
        return file_ops.get_dataframe(context=target_file_context, delimiter=delimiter, engine='python')

    def get_partition_values(self, file_name: str, fmt: str) -> (str, str, str):
        as_of_date = get_filename_timestamp(file_name, fmt)
        year = as_of_date.strftime('%Y')
        month = as_of_date.strftime('%m')
        day = as_of_date.strftime('%d')
        return year, f"{year}{month}", f"{year}{month}{day}"
