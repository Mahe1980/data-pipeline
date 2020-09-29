from glue_file_processing.src.glue_file_processing.process_steps import ProcessSteps
from glue_file_processing.src.glue_file_processing.processor import ErrorReportLevel


def test_get_error_key():
    file_key = 'CFM/Everest/Holdings/Automated/Holdings20200205233143_Empty.csv'
    detailed_error_key = ProcessSteps(None).get_error_key(file_key, '%Y%m%d%H%M%S', ErrorReportLevel.DETAILED)
    summary_error_key = ProcessSteps(None).get_error_key(file_key, '%Y%m%d%H%M%S', ErrorReportLevel.SUMMARY)
    expected_detailed_error_key = 'CFM/error_reporting/detailed/year=2020/year_month=202002/process=Everest/data_source=Holdings/Holdings20200205233143_Empty.parquet.snappy'
    expected_summary_error_key = 'CFM/error_reporting/summary/year=2020/year_month=202002/process=Everest/data_source=Holdings/Holdings20200205233143_Empty.parquet.snappy'
    assert detailed_error_key == expected_detailed_error_key
    assert summary_error_key == expected_summary_error_key
