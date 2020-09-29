from typing import List
from boto3 import Session
from gluecatalog.data_types import DataTypes
from gluecatalog.datacatalog_manager import DataCatalogManager
from gluecatalog.model import ParquetFormat, Table, Column

from .logger import get_logger
from .pipeline_enum import CatalogDatabase
from .pipeline_parquet import PipelineParquet
from .process_steps import ProcessSteps
from .util.path import get_root_folder
from .config import Config

LOGGER = get_logger()


class PipelineCatalog:
    def __init__(self, session: Session):
        self.__session = session
        self._pipeline_parquet = PipelineParquet(self.__session)
        self._process_step = ProcessSteps(self.__session)
        self.catalog_manager = DataCatalogManager(self.__session)

    def __get_partition_path(self, target_bucket: str, target_key: str, filename_timestamp_fmt: str):
        parquet_key = self._pipeline_parquet.get_parquet_key(target_key, filename_timestamp_fmt)
        parquet_key = f"s3://{target_bucket}/{parquet_key}"
        return parquet_key[:parquet_key.rindex("/")]

    @staticmethod
    def __get_partition_folder_path(target_bucket: str, target_key: str):
        root_folder = get_root_folder(target_key)
        return f"s3://{target_bucket}/{root_folder}/parquet/"

    @staticmethod
    def __get_partition_columns() -> List[Column]:
        return [Column("year", DataTypes.STRING), Column("year_month", DataTypes.STRING),
                Column('year_month_day', DataTypes.STRING)]

    def __get_partition_values(self, target_key: str, filename_timestamp_fmt: str):
        year, year_month, year_month_day = self._pipeline_parquet.get_partition_values(
            target_key, filename_timestamp_fmt)
        return [year, year_month, year_month_day]

    def __get_table(self, target_bucket: str, target_key: str, pipeline_name: str, config: Config) -> Table:
        cols_flattened = config.catalog_columns(catalog_columns=True)
        partition_columns = self.__get_partition_columns()
        partition_folder_path = self.__get_partition_folder_path(target_bucket, target_key)
        table = Table(name=pipeline_name, columns=cols_flattened, s3_location=partition_folder_path,
                      storage_format=ParquetFormat(), partition_cols=partition_columns,
                      description=pipeline_name)
        return table

    def update_glue_catalog(self, target_bucket: str, target_key: str, pipeline_name: str, config: Config):
        table = self.__get_table(target_bucket, target_key, pipeline_name, config)
        self.catalog_manager.sync_table_definition(CatalogDatabase.Curated.value, table)
        partition_path = self.__get_partition_path(target_bucket, target_key, config.filename_timestamp_fmt)
        partition_values = self.__get_partition_values(target_key, config.filename_timestamp_fmt)
        self.catalog_manager.add_table_partition(CatalogDatabase.Curated.value, table, partition_values, partition_path)
        LOGGER.info(f"Updated glue catalog for {pipeline_name}")

    def is_table_needing_updating_schema(self, database_name: str, table_name: str, current_table_cols: List[str],
                                         current_partition_values: List[str]):

        return self.catalog_manager.is_table_needing_updating_schema(database_name, table_name, current_table_cols,
                                                                     current_partition_values)
