"""Functions to add new columns to data frames.
This should only contain simple extensions and not enrichment
e.g. if you need to join two dataframes to calculate the new value then it does not belong here
"""
from datetime import datetime
from decimal import Decimal
from typing import Dict, Union

import pandas as pd

from ..config import AsOfDateConfig, ColumnName, AsOfDateSource, ColumnConfig


def with_as_of_date(
        df: pd.DataFrame,
        as_of_date_config: AsOfDateConfig,
        timestamp: datetime) -> pd.DataFrame:
    if as_of_date_config.source == AsOfDateSource.DataframeColumn:
        return with_as_of_date_from_dataset(df, as_of_date_config.source_column)
    if as_of_date_config.source == AsOfDateSource.FileTimestamp:
        return with_as_of_date_from_timestamp(df, timestamp)
    raise NotImplementedError(as_of_date_config.source)


def with_error_columns(df: pd.DataFrame,
                       error_indexes: pd.DataFrame,
                       correlation_id: str,
                       schema_config: Dict[str, ColumnConfig]):
    # make a copy of the original data frame in order to maintain the guarantee of not changing data we are passed
    df = df = df.copy(deep=True)
    # we only care if there have been errors, not how many so we ignore multiple errors on the same row
    error_indexes = error_indexes.drop_duplicates(inplace=False)

    # set error index to row index so we can merge with main data frame by it
    error_indexes.set_index(ColumnName.ROW_INDEX, inplace=True)
    error_indexes[ColumnName.CONFIDENCE_LEVEL] = Decimal(0)

    # set the row index and correlation position, probably to the end of the result
    _insert_column_at_schema_position_inplace(df, schema_config, ColumnName.ROW_INDEX, df.index)
    _insert_column_at_schema_position_inplace(df, schema_config, ColumnName.CORRELATION_ID, correlation_id)

    # set a default confidence level of 1, we will update with the error information at the next step
    _insert_column_at_schema_position_inplace(df, schema_config, ColumnName.CONFIDENCE_LEVEL, Decimal(1))

    # if we encountered any errors, set the confidence level to zero
    df.update(error_indexes)

    return df


def with_as_of_date_from_timestamp(
        df: pd.DataFrame,
        timestamp: datetime) -> pd.DataFrame:
    asofdate_series = pd.DataFrame(timestamp.date(), columns=[ColumnName.AS_OF_DATE], index=df.index)
    if ColumnName.AS_OF_DATE in df.columns:
        df = df.drop(ColumnName.AS_OF_DATE, axis=1, inplace=False)
    return pd.concat([asofdate_series, df], axis=1)


def with_as_of_date_from_dataset(
        df: pd.DataFrame,
        source_column: str) -> pd.DataFrame:
    if source_column not in df.columns:
        raise ValueError(f"Column {source_column} not in data set columns '{list(df.columns)}.")
    return df.rename(columns={source_column: ColumnName.AS_OF_DATE})


def _insert_column_at_schema_position_inplace(
        df: pd.DataFrame,
        schema_config: Dict[str, ColumnConfig],
        column_name: str,
        value: Union[pd.Series, str, Decimal]):
    column_loc = _column_position(schema_config, column_name)
    df.insert(loc=column_loc, column=column_name, value=value)


def _column_position(schema_config: Dict[str, ColumnConfig], column_name: str) -> int:
    # this checks for the sanitized column name position!
    column_names = list(schema_config.keys())
    return column_names.index(column_name)
