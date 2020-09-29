from typing import Dict, Union, List
from dataclasses import dataclass
from enum import IntEnum, auto

from gluecatalog.sanitize import sanitize_column_name
from gluecatalog.data_types import DataTypes
from gluecatalog.model import Column

from .transform import ColumnName


class ConfigKey:
    AS_OF_DATE_COLUMN = 'asofdate'
    DELIMITER = 'delimiter'
    FILE_NAME_TIME_FMT = 'file_name_timestamp'
    SCHEMA = 'schema'
    SANITIZE_COLUMNS = 'sanitize_columns'
    DATA_QUALITY_THRESHOLD = 'threshold'
    DATA_QUALITY_WEIGHT = 'weight'
    DATA_QUALITY_REPORTING = 'error_reporting'
    DATA_QUALITY_RULES = 'rules'
    ERROR_REPORTING_ENABLED = 'error_reporting_enabled'


class ConfigDefault:
    DELIMITER = ','
    FILE_NAME_TIME_FMT = '%Y%m%d%H%M%S'
    SANITIZE_COLUMNS = False
    DATA_QUALITY_THRESHOLD = 0.0
    DATA_QUALITY_WEIGHT = 0.0  # this makes more sense to be 1.0, but current defaults are 0 everywhere
    DATA_QUALITY_REPORTING = dict()
    DATA_QUALITY_RULES = dict()
    ERROR_REPORTING_ENABLED = True


@dataclass
class DataQualityRuleConfig:
    config: Dict
    threshold: float
    weight: float


class AsOfDateSource(IntEnum):
    FileTimestamp = auto()
    DataframeColumn = auto()


@dataclass
class AsOfDateConfig():
    source: AsOfDateSource
    source_column: Union[str, None]


@dataclass
class ColumnConfig():
    name_in_file: str
    data_type: str
    is_calculated: bool


class Config():
    def __init__(self, pipeline_config: Dict):
        """Parses and exposes pipeline configuration

        Arguments:
            pipeline_config {Dict} -- Source configuration values.
        """

        self.delimiter: str = pipeline_config.get(ConfigKey.DELIMITER, ConfigDefault.DELIMITER)
        """The field delimiter used in the source file"""

        self.filename_timestamp_fmt: str = pipeline_config.get(
            ConfigKey.FILE_NAME_TIME_FMT, ConfigDefault.FILE_NAME_TIME_FMT)
        """The format of the timestamp provided in the name of the source file"""

        self.sanitize_columns: bool = pipeline_config.get(ConfigKey.SANITIZE_COLUMNS, ConfigDefault.SANITIZE_COLUMNS)
        """Will column names be conformed to snake case"""

        self.error_reporting_enabled: bool = pipeline_config.get(
            ConfigKey.ERROR_REPORTING_ENABLED, ConfigDefault.ERROR_REPORTING_ENABLED)
        """Will context columns (correlation id etc.) be added to the parquet file"""

        self.schema: Dict[str, ColumnConfig] = self._parse_schema(pipeline_config)
        """A mapping of sanitized column names to configuration (data types etc.)"""

        self.as_of_date: AsOfDateConfig = self._parse_as_of_date(pipeline_config)
        """The configuration for calculating "as of" dates at the row level"""

        self.data_quality_rules: Dict[str, DataQualityRuleConfig] = self._parse_data_quality_rules(pipeline_config)
        """A mapping of data quality rule names to their configuration"""

        self.data_quality_reporting: Dict[str, str] = self._parse_data_quality_reporting(pipeline_config)
        """A mapping of sanitized columns to reporting columns that will appear in data quality reports"""

    def filtered_schema(self, include_calculated_cols: bool = True) -> Dict[str, ColumnConfig]:

        def should_include(col_config: ColumnConfig):
            if include_calculated_cols:
                return True

            return not col_config.is_calculated

        return {
            col_name: col_config
            for col_name, col_config in self.schema.items()
            if should_include(col_config)
        }

    def catalog_columns(self, include_calculated_cols: bool) -> List[Column]:
        columns: Dict[str, ColumnConfig] = self.filtered_schema(include_calculated_cols)

        def create_column(col_name: str, col_config: ColumnConfig):
            name = col_name if self.sanitize_columns else col_config.name_in_file
            col_type = DataTypes.from_string(col_config.data_type)
            return Column(name, col_type)

        col_objects = [create_column(col_name, col_config) for col_name, col_config in columns.items()]

        return col_objects

    def filtered_deprecated_confg(self, pipeline_config) -> Dict:
        selected_cols = [
            col_config.name_in_file
            for col_config
            in self.filtered_schema(include_calculated_cols=False).values()]
        app_config = pipeline_config.copy()
        app_config['schema'] = [c for c in app_config['schema'] if c['name'] in selected_cols]
        return app_config

    def _parse_schema(self, pipeline_config: Dict) -> Dict[str, ColumnConfig]:
        return {
            sanitize_column_name(col_config['name']) if self.sanitize_columns else col_config['name']:
            ColumnConfig(
                name_in_file=col_config['name'],
                data_type=col_config['type'],
                is_calculated=col_config.get('is_calculated', False)
            )
            for col_config
            in pipeline_config['schema']
        }

    def _parse_as_of_date(self, pipeline_config: Dict) -> AsOfDateConfig:
        if ConfigKey.AS_OF_DATE_COLUMN in pipeline_config:
            as_of_date_source_column = pipeline_config[ConfigKey.AS_OF_DATE_COLUMN]
            if as_of_date_source_column not in self.schema:
                raise ValueError(
                    f"Column '{as_of_date_source_column}' not found in "
                    f"configured schema with columns: {list(self.schema.keys())}")
            return AsOfDateConfig(
                AsOfDateSource.DataframeColumn,
                as_of_date_source_column)

        if ColumnName.AS_OF_DATE in self.schema:
            return AsOfDateConfig(
                AsOfDateSource.FileTimestamp,
                None
            )

        raise ValueError(f"Cannot find configuration for as of date column.")

    def _parse_data_quality_rules(self, pipeline_config: Dict) -> DataQualityRuleConfig:
        def parse_data_quality_rule_config(rule_config: Dict):
            assert len(rule_config) == 1
            name = list(rule_config.keys())[0]

            config: Dict = rule_config[name].copy()
            threshold = config.pop(ConfigKey.DATA_QUALITY_THRESHOLD)
            weight = config.pop(ConfigKey.DATA_QUALITY_WEIGHT)

            # whatever is left is the actual rule config, the name of the element varies by rule
            inner_rule_config = config

            return (name,
                    DataQualityRuleConfig(
                        config=inner_rule_config,
                        threshold=threshold,
                        weight=weight
                    ))

        return dict(
            parse_data_quality_rule_config(rule_config)
            for rule_config
            in pipeline_config.get(ConfigKey.DATA_QUALITY_RULES, ConfigDefault.DATA_QUALITY_RULES))

    def _parse_data_quality_reporting(self, pipeline_config: Dict) -> Dict[str, str]:
        config = pipeline_config.get(ConfigKey.DATA_QUALITY_REPORTING, ConfigDefault.DATA_QUALITY_REPORTING)
        for destination_column, source_column in config.items():
            if source_column not in self.schema:
                raise ValueError(
                    f"Column '{source_column}' not found in configured schema with columns: {list(self.schema.keys())}")
            if destination_column in self.schema and source_column != destination_column:
                raise ValueError(
                    f"Reporting column to be added '{source_column}' is also found in "
                    f"configured schema with columns: {list(self.schema.keys())}")
        return config
