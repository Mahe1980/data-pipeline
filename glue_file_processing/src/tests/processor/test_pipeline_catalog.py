from typing import List
from unittest.mock import MagicMock
import types

from gluecatalog.data_types import DataTypes
from gluecatalog.model import Column, Table
from gluecatalog.sanitize import sanitize_column_name

from glue_file_processing.src.glue_file_processing.pipeline_catalog import PipelineCatalog
from glue_file_processing.src.glue_file_processing.pipeline_configuration import CONFIGURATION
from glue_file_processing.src.glue_file_processing.config import Config


config = Config(CONFIGURATION)


class TestData:
    expected_column_names = CONFIGURATION['schema']
    expected_column_names = [sanitize_column_name(x["name"]) if CONFIGURATION.get('sanitize_columns') else x["name"]
                             for x in expected_column_names]


class TestCatalogManager:
    def add_table_partition(self, database_name: str, table: Table, partition_values: List[str], partition_path):
        pass

    def sync_table_definition(self, database_name: str, table: Table):
        expected_column_names = TestData.expected_column_names + ["extra_column"]
        column_names = [col.name for col in table.columns]
        assert expected_column_names is not column_names


class TestPipelineCatalog:
    session = MagicMock(autoSpec=True)
    pipeline_catalog = PipelineCatalog(session)
    pipeline_catalog.catalog_manager = TestCatalogManager()

    def test_column_order(self):
        columns = config.catalog_columns(include_calculated_cols=True)
        actual_column_names = [col.name for col in columns]
        assert actual_column_names == TestData.expected_column_names

    def test_column_added_at_the_end(self):
        columns = config.catalog_columns(include_calculated_cols=True)
        columns.insert(1, Column("extra_column", DataTypes.STRING))

        def incorrect_column_order(self, catalog_columns):
            return columns
        config.catalog_columns = types.MethodType(incorrect_column_order, config)
        target_key = "BusinessArea/BusinessProcess/DataSource/Automated/test1_20191003103000.txt"
        self.pipeline_catalog.update_glue_catalog("target_bucket", target_key, "test_pipeline", config)
