from datetime import datetime, date
import decimal

from pytest import raises

import pandas as pd

from glue_file_processing.src.glue_file_processing.transform.extend import (
    with_as_of_date,
    with_error_columns)
from glue_file_processing.src.glue_file_processing.config import AsOfDateConfig, AsOfDateSource, ColumnName, ColumnConfig


def test_with_as_of_date_from_timestamp_empty():
    as_of_date_config = AsOfDateConfig(
        source=AsOfDateSource.FileTimestamp,
        source_column=None
    )
    timestamp = datetime.strptime('1983-12-09', '%Y-%m-%d')

    df = pd.DataFrame()
    extended_df = with_as_of_date(df, as_of_date_config, timestamp)
    assert ColumnName.AS_OF_DATE in extended_df.columns


def test_with_as_of_date_from_dataset_empty():
    as_of_date_config = AsOfDateConfig(
        source=AsOfDateSource.DataframeColumn,
        source_column='when_it_happened'
    )

    df = pd.DataFrame(columns=['when_it_happened'])
    extended_df = with_as_of_date(df, as_of_date_config, None)
    assert ColumnName.AS_OF_DATE in extended_df.columns
    assert 'when_it_happened' not in extended_df.columns


def test_with_as_of_date_unknow_method():
    with raises(NotImplementedError):
        with_as_of_date(pd.DataFrame.empty, AsOfDateConfig(None, None), None)


def test_with_as_of_date_from_dataset_col_missing():
    as_of_date_config = AsOfDateConfig(
        source=AsOfDateSource.DataframeColumn,
        source_column='when_it_happened'
    )

    df = pd.DataFrame(columns=['when_it_did_not_happen'])
    with raises(ValueError):
        with_as_of_date(df, as_of_date_config, None)


def test_with_as_of_date_from_timestamp_happy_path():
    as_of_date_config = AsOfDateConfig(
        source=AsOfDateSource.FileTimestamp,
        source_column=None
    )
    timestamp = datetime.strptime('1983-12-09', '%Y-%m-%d')
    as_of_date = timestamp.date()

    df = pd.DataFrame([
        {'a': 1},
        {'a': 2}
    ])
    extended_df = with_as_of_date(df, as_of_date_config, timestamp)

    expected_df = pd.DataFrame([
        {ColumnName.AS_OF_DATE: as_of_date, 'a': 1},
        {ColumnName.AS_OF_DATE: as_of_date, 'a': 2}
    ])

    pd.testing.assert_frame_equal(expected_df, extended_df)


def test_with_as_of_date_from_dataset_happy_path():
    as_of_date_config = AsOfDateConfig(
        source=AsOfDateSource.DataframeColumn,
        source_column='when_it_happened'
    )
    as_of_date = datetime.strptime('1983-12-09', '%Y-%m-%d').date()

    df = pd.DataFrame([
        {'a': 1, 'when_it_happened': as_of_date},
        {'a': 2, 'when_it_happened': as_of_date}
    ])
    extended_df = with_as_of_date(df, as_of_date_config, None)

    expected_df = pd.DataFrame([
        {'a': 1, ColumnName.AS_OF_DATE: as_of_date},
        {'a': 2, ColumnName.AS_OF_DATE: as_of_date}
    ])

    pd.testing.assert_frame_equal(expected_df, extended_df)


def test_add_asofdate_field():
    df = pd.DataFrame([{"Name": "Test", "Amount": decimal.Decimal("20.34"),
                        "DateString": '2019-11-11T14:20:00', 'Extra_Column': 'some-value'},
                       {"Name": "Test2", "Amount": decimal.Decimal("20.34"), "DateString": '2019-11-11T14:20:00',
                        'Extra_Column': 'some-value2'}
                       ])
    as_of_date_config = AsOfDateConfig(
        source=AsOfDateSource.FileTimestamp,
        source_column='when_it_happened'
    )
    timestamp = datetime.strptime('2019-01-02', '%Y-%m-%d')

    extended_df = with_as_of_date(df, as_of_date_config, timestamp)
    expected_df = pd.DataFrame([
        {'asofdate': date(2019, 1, 2), "Name": "Test", "Amount": decimal.Decimal("20.34"),
         "DateString": '2019-11-11T14:20:00', "Extra_Column": 'some-value'},
        {'asofdate': date(2019, 1, 2), "Name": "Test2", "Amount": decimal.Decimal("20.34"),
         "DateString": '2019-11-11T14:20:00', 'Extra_Column': 'some-value2'}
    ])

    pd.testing.assert_frame_equal(expected_df, extended_df)
    assert ['asofdate', 'Name', 'Amount', 'DateString', 'Extra_Column'] == extended_df.columns.values.tolist()


def test_add_asofdate_timestamp_col_exists():
    df = pd.DataFrame([{"Name": "Test", "Amount": decimal.Decimal("20.34"),
                        "asofdate": '2019-11-11T14:20:00', 'Extra_Column': 'some-value'},
                       {"Name": "Test2", "Amount": decimal.Decimal("20.34"), "asofdate": '2019-11-11T14:20:00',
                        'Extra_Column': 'some-value2'}
                       ])
    as_of_date_config = AsOfDateConfig(
        source=AsOfDateSource.FileTimestamp,
        source_column='asofdate'
    )
    timestamp = datetime.strptime('2019-01-02', '%Y-%m-%d')

    extended_df = with_as_of_date(df, as_of_date_config, timestamp)
    expected_df = pd.DataFrame([
        {'asofdate': date(2019, 1, 2), "Name": "Test", "Amount": decimal.Decimal("20.34"),
         "Extra_Column": 'some-value'},
        {'asofdate': date(2019, 1, 2), "Name": "Test2", "Amount": decimal.Decimal("20.34"),
         'Extra_Column': 'some-value2'}
    ])

    pd.testing.assert_frame_equal(expected_df, extended_df)
    assert ['asofdate', 'Name', 'Amount', 'Extra_Column'] == extended_df.columns.values.tolist()


def test_with_error_columns_empty():
    df = pd.DataFrame()
    schema_config = {
        'row_index': ColumnConfig('row_index', 'INT', True),
        'correlation_id': ColumnConfig('correlation_id', 'INT', True),
        'confidence_level': ColumnConfig('confidence_level', 'INT', True),
    }
    error_indexes = pd.DataFrame(columns=[ColumnName.ROW_INDEX])
    correlation_id = ''

    with_error_columns(df, error_indexes, correlation_id, schema_config)


def test_with_error_columns_all_wrong():
    df = pd.DataFrame([
        {'a': 1, 'b': 'x'},
        {'a': 2, 'b': 'y'}])
    error_indexes = pd.DataFrame([
        {'row_index': 0},
        {'row_index': 1}])
    schema_config = {
        'row_index': ColumnConfig('row_index', 'INT', True),
        'correlation_id': ColumnConfig('correlation_id', 'INT', True),
        'confidence_level': ColumnConfig('confidence_level', 'INT', True),
    }
    dfe = with_error_columns(df, error_indexes, '1', schema_config)

    assert list(dfe['confidence_level']) == [0, 0]


def test_with_confidence_level_all_correct():
    df = pd.DataFrame([{'a': 1, 'b': 'x'}, {'a': 2, 'b': 'y'}])
    error_indexes = pd.DataFrame(columns=['row_index'])
    schema_config = {
        'row_index': ColumnConfig('row_index', 'INT', True),
        'correlation_id': ColumnConfig('correlation_id', 'INT', True),
        'confidence_level': ColumnConfig('confidence_level', 'INT', True),
    }
    dfe = with_error_columns(df, error_indexes, 'a', schema_config)

    assert list(dfe['confidence_level']) == [1, 1]
    assert list(dfe['row_index']) == [0, 1]
    assert list(dfe['correlation_id']) == ['a', 'a']


def test_with_error_columns_mixed():
    df = pd.DataFrame([
        {'a': 1, 'b': 'x'},
        {'a': 2, 'b': 'y'}])
    error_indexes = pd.DataFrame([
        {'row_index': 0, 'row_index': 0}])
    schema_config = {
        'row_index': ColumnConfig('row_index', 'INT', True),
        'correlation_id': ColumnConfig('correlation_id', 'INT', True),
        'confidence_level': ColumnConfig('confidence_level', 'INT', True),
    }

    dfe = with_error_columns(df, error_indexes, 'a', schema_config)
    assert list(dfe['row_index']) == [0, 1]
    assert list(dfe['confidence_level']) == [0, 1]
    assert list(dfe['correlation_id']) == ['a', 'a']


def test_with_confidence_level_mixed():
    df = pd.DataFrame([{'a': 1, 'b': 'x'}, {'a': 2, 'b': 'y'}])
    error_indexes = pd.DataFrame([{'row_index': 1}, {'row_index': 1}])
    schema_config = {
        'row_index': ColumnConfig('row_index', 'INT', True),
        'correlation_id': ColumnConfig('correlation_id', 'INT', True),
        'confidence_level': ColumnConfig('confidence_level', 'INT', True),
    }

    dfe = with_error_columns(df, error_indexes, 'a', schema_config)
    assert list(dfe['row_index']) == [0, 1]
    assert list(dfe['confidence_level']) == [1, 0]
    assert list(dfe['correlation_id']) == ['a', 'a']
