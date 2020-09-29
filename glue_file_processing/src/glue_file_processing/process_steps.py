from datetime import datetime
from typing import Tuple, List, Dict
from os import path

import pandas as pd

from core.aws import AwsService, AwsRegion
from file_operations.context import Context as FileOperationsContext
from file_operations.file_operations import S3FileOperations
from serializer.parquet_writer import ParquetWriter
from file_operations.file_stream import ByteStream
from file_operations.s3_file_stream_builder import S3FileStreamBuilder
from hashing.stream_hash_generator import StreamHashGenerator
from hashing.supported_algorithm import SupportedAlgorithm
from sns.topics import Topic
from tagger.context import Context as TaggerContext
from tagger.tagger import Tagger
from data_quality.rule_manager import RuleManager

from .logger import get_logger
from .notification import Notification
from .payload_builder import PayloadBuilder
from .pipeline_enum import S3Object, TagKeys, SnsStatus
from .process_context import ProcessContext
from .pipeline_configuration import CONFIGURATION
from .util.path import get_file_name, get_root_folder, get_filename_timestamp
from .transform.extend import with_as_of_date, with_as_of_date_from_timestamp
from .config import Config, ColumnConfig

LOGGER = get_logger()


#  pylint:disable=too-many-public-methods
class ProcessSteps:
    def __init__(self, session):
        self._session = session

    def _get_file_stream(self, file_bucket: str, file_key: str) -> ByteStream:
        s3_stream_builder = S3FileStreamBuilder(
            self._session.client(AwsService.S3.value))
        s3_file_stream = s3_stream_builder.create_stream(
            bucket_name=file_bucket, key=file_key)
        return s3_file_stream

    def delete_file(self, file_ops_context: FileOperationsContext):
        file_operations = S3FileOperations(self._session)
        file_operations.delete(file_ops_context)
        LOGGER.info(
            f"Deleted file '{file_ops_context.file_bucket}/{file_ops_context.file_key}'")

    def move_file(self, file_ops_context: FileOperationsContext):
        self.copy_file(file_ops_context)
        self.delete_file(file_ops_context)

    def copy_file(self, file_ops_context: FileOperationsContext):
        S3FileOperations(self._session).copy(file_ops_context)
        LOGGER.info(f"Copied file from '{file_ops_context.file_bucket}/{file_ops_context.file_key}' to "
                    f"'{file_ops_context.target_bucket}/{file_ops_context.target_key}'")

    def write_detailed_error_report(self, data_frames, bucket, key):
        if all(df.empty for df in data_frames):
            return pd.DataFrame(columns=['row_index'])

        df = ParquetWriter(self._session).write_parquet_file_noschema(
            data_frames=data_frames,
            bucket=bucket,
            key=key
        )
        error_count = len(df.index)
        if error_count > 0:
            LOGGER.info(f"Wrote details for {error_count} data quality errors to s3://{bucket}/{key}.")
        else:
            LOGGER.info(f"There were no data quality errors so no error details file was written.")

        return df

    def write_summary_error_report(self, summary_df, bucket, key, config):
        timestamp = get_filename_timestamp(key, config.filename_timestamp_fmt)
        summary_df = with_as_of_date_from_timestamp(summary_df, timestamp)

        ParquetWriter(self._session).write_parquet_file_noschema(
            data_frames=[summary_df],
            bucket=bucket,
            key=key
        )

        LOGGER.info(f"Wrote data quality summary to s3://{bucket}/{key}.")

        return summary_df

    def get_version_id(self, target_bucket: str, target_key: str):
        obj = self._session.client(AwsService.S3.value).get_object(
            Bucket=target_bucket,
            Key=target_key
        )
        return obj[S3Object.VersionId.value]

    def calculate_hash(self, file_bucket: str, file_key: str) -> str:
        s3_file_stream = self._get_file_stream(file_bucket=file_bucket, file_key=file_key)
        stream_hash_generator = StreamHashGenerator()
        hash_value = stream_hash_generator.get_hash_digest(
            s3_file_stream, SupportedAlgorithm.SHA256)
        LOGGER.info(f"Calculated hash for '{file_bucket}/{file_key}'")
        return hash_value

    def tag_file(self, tags: dict, file_bucket: str, file_key: str, file_version_id: str):
        tagger = Tagger(session=self._session, allowed_tags=[
            key.value for key in TagKeys])
        tagger_context = TaggerContext(
            file_key=file_key, file_bucket=file_bucket, file_version_id=file_version_id)
        tagger.put_tags(tags, tagger_context)
        tag_keys = ', '.join(tags.keys())
        LOGGER.info(f"Tagged file '{file_bucket}/{file_key}' with {tag_keys}")

    def check_data_quality(
            self,
            file_bucket: str,
            file_key: str,
            file_version: str,
            correlation_id: str,
            processed_on: datetime,
            config: Config):
        file_ops = S3FileOperations(session=self._session)
        file_context = FileOperationsContext(file_key, file_bucket)
        source_df = file_ops.get_dataframe(context=file_context, delimiter=config.delimiter, engine='python')
        timestamp = get_filename_timestamp(file_key, config.filename_timestamp_fmt)
        source_df_with_asofdate = with_as_of_date(source_df, config.as_of_date, timestamp)

        return RuleManager.check_data_quality(
            file_bucket=file_bucket,
            file_key=file_key,
            source_data_frame=source_df_with_asofdate,
            file_version=file_version,
            correlation_id=correlation_id,
            processed_on=processed_on,
            app_config=config.filtered_deprecated_confg(CONFIGURATION))

    def send_notification(self, context: ProcessContext, topic: Topic, region_name: AwsRegion, status: SnsStatus,
                          subject: str, msg: str, email_to: str):
        payload = PayloadBuilder(context).build_payload(topic=topic, region_name=region_name, status=status,
                                                        subject=subject, msg=msg, email_to=email_to)
        Notification.notification(
            self._session, payload, context.correlation_id)

    def get_target_key(self, file_key: str, fmt: str) -> str:
        root_folder = get_root_folder(file_key)
        file_name = get_file_name(file_key)
        year, month, _ = self._extract_dateparts(file_name, fmt)
        target_key = f"{root_folder}/{year}/{year}{month}/{file_name}"
        LOGGER.info(f"Calculated target key '{target_key}'")
        return target_key

    def get_error_key(self, file_key: str, fmt: str, level: str) -> str:
        business_area, business_process, data_source = self.extract_business_details(file_key)
        file_name = get_file_name(file_key)
        file_name_noext = path.splitext(file_name)[0]
        year, month, _ = self._extract_dateparts(file_name, fmt)
        area_error_root = f'{business_area}/error_reporting/{level}'
        target_key = (
            f"{area_error_root}/year={year}/year_month={year}{month}/"
            f"process={business_process}/data_source={data_source}/"
            f"{file_name_noext}.parquet.snappy")
        LOGGER.info(f"Calculated {level} error key '{target_key}'")
        return target_key

    def _extract_dateparts(self, file_name: str, fmt: str) -> Tuple[str, str, str]:
        as_of_date = get_filename_timestamp(file_name, fmt)
        year = as_of_date.strftime('%Y')
        month = as_of_date.strftime('%m')
        day = as_of_date.strftime('%d')
        return year, month, day

    def extract_business_details(self, file_key: str):
        root_folder = get_root_folder(file_key)
        root_parts = root_folder.split('/')
        return root_parts[0], root_parts[1], root_parts[2]

    @staticmethod
    def error_table_name(business_area: str, reporting_level: str):
        return f"{business_area.lower()}_errors_{reporting_level.lower()}"

    def extract_detail_partition_values(self, file_key: str, fmt: str) -> List[str]:
        root_folder = get_root_folder(file_key)
        root_parts = root_folder.split('/')
        file_name = get_file_name(file_key)
        year, month, _ = self._extract_dateparts(file_name, fmt)
        return [year, f"{year}{month}", root_parts[1], root_parts[2]]

    @staticmethod
    def is_calculated_column(col_info: Dict) -> bool:
        return col_info.get("is_calculated", "false").lower() == "true"
