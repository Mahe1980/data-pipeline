# pylint:disable=invalid-name,no-self-use
from datetime import datetime

import pandas as pd
from core.aws import AwsRegion
from data_quality.data_quality_const import QualityRuleConst
from data_quality.model import RuleResult, CheckResult, RuleStatus
from moto import mock_s3
from pytest import mark

from glue_file_processing.src.glue_file_processing.boto_session import BotoSession
from glue_file_processing.src.glue_file_processing.pipeline_configuration import CONFIGURATION
from glue_file_processing.src.glue_file_processing.process_steps import ProcessSteps
from glue_file_processing.src.glue_file_processing.config import Config


config = Config(CONFIGURATION)
# assume for testing that the last parameter is the column list
data_columns = CONFIGURATION['schema']
data_columns = [x["name"] for x in data_columns if not bool(x.get("is_calculated", False))]
error_df_columns = [
    'row_index',
    'rule_name',
    'rule_description',
    'processed_on',
    's3_key',
    's3_version_id',
    'correlation_id',
    'asofdate',
    'business_unit',
    'portfolio',
    'company'
]

test_dq_success = [
    ({"file_key": "a/b/c20190101010101.txt", "file_bucket": "tests"}, data_columns,
     CheckResult(1.0, [
         RuleResult(RuleStatus.Pass, 1.0, QualityRuleConst.BLANK, pd.DataFrame(columns=['blank_row_index'])),
         RuleResult(RuleStatus.Pass, 1.0, QualityRuleConst.COLUMN_COUNT),
         RuleResult(RuleStatus.Pass, 1.0, QualityRuleConst.COLUMN_NAMES),
         RuleResult(RuleStatus.Pass, 1.0, QualityRuleConst.OUTLIERS),
         RuleResult(RuleStatus.Pass, 1.0, QualityRuleConst.DATA_TYPES),
         RuleResult(RuleStatus.Pass, 1.0, QualityRuleConst.CONDITION_CHECK,
                    pd.DataFrame(columns=['row_index',
                                          'condition_column',
                                          'condition_column_value',
                                          'condition_operator',
                                          'condition_operand',
                                          'condition_operand_value',
                                          'test_column',
                                          'test_column_value',
                                          'test_operator',
                                          'test_operand',
                                          'test_operand_value'])),
         RuleResult(RuleStatus.Pass, 1.0, QualityRuleConst.PRIMARY_ID),
         RuleResult(RuleStatus.Pass, 1.0, QualityRuleConst.DUPLICATES),
     ])
     )
]

error_reporting_biz_cols = list(CONFIGURATION['error_reporting'].values())
dq_error_cols = list(set(data_columns[:20] + error_reporting_biz_cols))

test_dq_error = [
    ({"file_key": "a/b/c20190101010101.txt", "file_bucket": "tests"},
     dq_error_cols)
]


@mock_s3
class TestDataQuality:
    @mark.parametrize("input_data, file_data, expected", test_dq_success)
    def test_dq_success(self, s3_helper, input_data, file_data, expected):
        s3_helper.create_bucket(file_bucket=input_data["file_bucket"])
        s3_helper.create_file_key(file_bucket=input_data["file_bucket"], file_key=input_data["file_key"],
                                  file_data=bytes(','.join(file_data), encoding='utf8'))
        check_result = ProcessSteps(BotoSession().get_session(AwsRegion.EUIreland)).check_data_quality(
            file_bucket=input_data["file_bucket"],
            file_key=input_data["file_key"],
            file_version='1',
            correlation_id='1',
            processed_on=datetime.now(),
            config=config)
        assert check_result.overall_score == expected.overall_score
        assert check_result.overall_status() == expected.overall_status()
        for col_name in error_df_columns:
            for error_result in check_result.rule_results:
                assert col_name in error_result.errors_df.columns
        assert [(r.score, r.status, r.rule_name) for r in check_result.rule_results] == [
            (r.score, r.status, r.rule_name) for r in expected.rule_results]

    @mark.parametrize("input_data, file_data", test_dq_error)
    def test_dq_error(self, s3_helper, input_data, file_data):
        s3_helper.create_bucket(file_bucket=input_data["file_bucket"])
        s3_helper.create_file_key(file_bucket=input_data["file_bucket"], file_key=input_data["file_key"],
                                  file_data=bytes(','.join(file_data), encoding='utf8'))

        result = ProcessSteps(BotoSession().get_session(AwsRegion.EUIreland)).check_data_quality(
            file_bucket=input_data["file_bucket"],
            file_key=input_data["file_key"],
            file_version='1',
            correlation_id='1',
            processed_on=datetime.now(),
            config=config)
        executed_rules = [r.rule_name for r in result.rule_results]
        assert [QualityRuleConst.BLANK, QualityRuleConst.COLUMN_COUNT, QualityRuleConst.COLUMN_NAMES,
                QualityRuleConst.OUTLIERS, QualityRuleConst.DATA_TYPES, QualityRuleConst.CONDITION_CHECK,
                QualityRuleConst.PRIMARY_ID, QualityRuleConst.DUPLICATES] == executed_rules
        error_rules = [r for r in result.rule_results if r.status == RuleStatus.Error]
        assert [QualityRuleConst.OUTLIERS, QualityRuleConst.DATA_TYPES] == [r.rule_name for r in error_rules]
        assert len(
            [r.exception for r in error_rules if r.exception]) == 2, "expected error reported, but not found"

    def test_with_broken_file(self, s3_helper):
        s3_helper.create_bucket(file_bucket='test')
        with open("tests/sample/Holdings20191206121315.csv", "rb") as sample_file:
            s3_helper.create_file_key(file_bucket='test', file_key='a/b/c20190101010101.txt',
                                      file_data=sample_file.read())

        result = ProcessSteps(BotoSession().get_session(AwsRegion.EUIreland)).check_data_quality(
            file_bucket='test',
            file_key='a/b/c20190101010101.txt',
            file_version="1",
            correlation_id="1",
            processed_on=datetime.now(),
            config=config)

        primary_id_variation_score = \
            [result.score for result in result.rule_results if result.rule_name == QualityRuleConst.PRIMARY_ID][0]

        duplicates_score = \
            [result.score for result in result.rule_results if result.rule_name == QualityRuleConst.DUPLICATES][0]

        assert primary_id_variation_score == 1.0
        assert duplicates_score == 0.6
