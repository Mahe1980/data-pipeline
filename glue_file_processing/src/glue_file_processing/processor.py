from traceback import extract_tb, format_list
from typing import List, Optional
from datetime import datetime
import pandas as pd

from core.aws import AwsRegion, AwsService
from file_operations.context import Context as FileOperationsContext
from file_operations.exceptions import MissingValueError
from sns.topics import Topic
from data_quality.model import CheckResult, RuleException, RuleStatus, RuleResult
from tagger.exceptions import TaggerException
from gluecatalog.crawler import start_crawler
from .boto_session import BotoSession
from .logger import get_logger
from .pipeline_catalog import PipelineCatalog
from .pipeline_configuration import CONFIGURATION
from .pipeline_enum import TagKeys, TagValues, SnsStatus, CatalogDatabase
from .pipeline_parquet import PipelineParquet
from .pipeline_process_folder import PipelineProcessFolder
from .process_context import ProcessContext
from .process_steps import ProcessSteps
from .config import Config


LOGGER = get_logger()

SUCCESS_MSG = "Successfully processed '{pipeline_name}' - '{business_process}' data is now available at '{uri}'. "
ERROR_MSG = "The business_process '{business_process}' has reported a failure for data provider '{pipeline_name}'." \
            "\r\n Please review file at '{uri}'."
UNKNOWN_ERROR_MSG = "The '{business_process}' has reported an unknown failure for data provider '{pipeline_name}'" \
                    " and file '{uri}'. <br /> Please see the exception below." \
                    "<br /><br />{exception}" \
                    "<br /><br />Traceback (most recent call last):<br /><pre>{stacktrace}</pre>"
CATALOG_ERROR_MSG = "Successfully processed '{pipeline_name}' - '{business_process}' data is now available at " \
                    "'{uri}'. However, it's unable to create Parquet/Catalog." \
                    "<br /> Please see the exception below." \
                    "<br /><br />{exception}" \
                    "<br /><br />Traceback (most recent call last):<br /><pre>{stacktrace}</pre>"
RULE_ERROR_MSG = "For {pipeline_name} and file {file}<br/>" \
                 "the following quality rules failed due to program error <br/>" \
                 "<pre>{rules}</pre>"
RULE_ERROR_SUBJECT = "{business_process} rule errors"
SUCCESS_SUBJECT = "{business_process} Successful"
ERROR_SUBJECT = "{business_process} Failed"
URI = "{s3_aws_service}://{file_bucket}/{file_key}"


class ErrorReportLevel:
    DETAILED = 'detailed'
    SUMMARY = 'summary'


class Processor:
    def __init__(self):
        self._session = BotoSession().get_session(AwsRegion.EUIreland)
        self._process_steps = ProcessSteps(self._session)
        self.pipeline_catalog = PipelineCatalog(self._session)
        self.config = Config(CONFIGURATION)

    def run(self, context: ProcessContext):
        pipeline_process_folder = PipelineProcessFolder(context.file_key, context.processed_on)
        try:
            self._tag_file_with_hash(file_bucket=context.file_bucket, file_key=context.file_key,
                                     file_version_id=context.file_version_id)
            self._copy_from_raw_to_to_be_processed(context, pipeline_process_folder.to_be_processed)

            self._tag_file_with_correlation_id(context)
            check_result = self._process_steps.check_data_quality(
                file_bucket=context.file_bucket,
                file_key=context.file_key,
                file_version=context.file_version_id,
                correlation_id=context.correlation_id,
                processed_on=datetime.strptime(context.processed_on, "%Y-%m-%dT%H:%M:%S.%fZ"),
                config=self.config)

            self._tag_file_with_quality_score(context, check_result)
            self._create_summary_error_report(context, check_result)
            error_indexes = self._create_detailed_error_report(context, check_result)
            self._handle_result(
                context,
                pipeline_process_folder,
                check_result.has_passed(),
                error_indexes=error_indexes)
            rule_errors = [result for result in check_result.rule_results if result.status == RuleStatus.Error]
            if rule_errors:
                self._send_rule_error_notification(context, rule_errors)

        except TaggerException as ex:
            LOGGER.exception(f"Pipeline failed due to tagging error: {ex}")
            self._handle_failed_pipeline(context, pipeline_process_folder)
        except RuleException as ex:
            LOGGER.exception(f"Unable to run Data Quality rules due to error: {ex}")
            self._handle_failed_pipeline(context, pipeline_process_folder)
        except MissingValueError as mve:
            LOGGER.exception(f"Unable to complete pipeline due to missing value {mve}")
            raise
        except Exception as ex:
            self._handle_unknown_errors(context, ex)
            raise

    def _handle_result(self, context: ProcessContext, pipeline_process_folder: PipelineProcessFolder, has_passed: bool,
                       error_indexes):
        if has_passed:
            target_key = self._handle_successful_quality_check(
                context=context, pipeline_process_folder=pipeline_process_folder)
            self._handle_catalog_parquet(context, target_key, error_indexes)
        else:
            self._handle_failed_pipeline(
                context=context, pipeline_process_folder=pipeline_process_folder)

    def _handle_catalog_parquet(self, context, target_key: str, error_indexes):
        parquet_key = None
        try:
            parquet_key = self._write_parquet_file(
                context=context, target_key=target_key, error_indexes=error_indexes)
            self.pipeline_catalog.update_glue_catalog(
                context.target_bucket,
                target_key,
                context.pipeline_name,
                self.config)
        except Exception as ex:  # pylint: disable=broad-except
            LOGGER.exception(f'Unable to create Parquet/Catalog: {ex}')
            self._delete_parquet_file(context.target_bucket, parquet_key)
            self._send_parquet_catalog_error_notification(context, ex)

    def _handle_successful_quality_check(self, context: ProcessContext, pipeline_process_folder: PipelineProcessFolder):
        try:
            LOGGER.info(
                f"Data Quality check passed for file '{context.file_bucket}/{context.file_key}'")
            target_key = self._find_target_key(
                context, self.config.filename_timestamp_fmt)
            self._move_file_from_to_be_processed_to_processed(
                context, pipeline_process_folder.processed)
            self._tag_file_with_processed(context)
            self._copy_file_from_processed_to_target(context, target_key)
        except Exception as ex:
            LOGGER.exception(
                f'Data Quality check has passed but unable to progress any further: {ex}')
            self._move_from_to_be_processed_to_error(
                context, pipeline_process_folder.error)
            self._tag_file_with_failed(context)
            self._handle_unknown_errors(context, ex)
            raise
        else:
            return target_key

    def _handle_failed_pipeline(self, context: ProcessContext, pipeline_process_folder: PipelineProcessFolder):
        try:
            LOGGER.info(f"Pipeline failed for file '{context.file_bucket}/{context.file_key}'")
            self._move_from_to_be_processed_to_error(
                context, pipeline_process_folder.error)
            self._tag_file_with_failed(context)
            self._send_pipeline_error_notification(context)
        except Exception as ex:  # pylint: disable=broad-except
            self._handle_unknown_errors(context, ex)

    def _handle_unknown_errors(self, context: ProcessContext,
                               exception: Exception):
        LOGGER.exception(f"Pipeline failed due to error {exception}")
        self._send_unknown_error_notification(
            context=context,
            exception=exception)

    def _calculate_hash(self, file_bucket: str, file_key: str):
        return self._process_steps.calculate_hash(file_bucket, file_key)

    def _tag_file_with_hash(self, file_bucket: str, file_key: str, file_version_id: str):
        hash_value = self._calculate_hash(file_bucket, file_key)
        self._process_steps.tag_file(
            {TagKeys.Hash.value: hash_value}, file_bucket=file_bucket, file_key=file_key,
            file_version_id=file_version_id)

    def _tag_file_with_correlation_id(self, context: ProcessContext):
        self._process_steps.tag_file(tags={TagKeys.CorrelationId.value: context.correlation_id},
                                     file_key=context.file_key, file_bucket=context.file_bucket,
                                     file_version_id=context.file_version_id)

    def _tag_file_with_processed(self, context: ProcessContext):
        tags = {TagKeys.Status.value: TagValues.Processed.value}
        self._process_steps.tag_file(tags, file_bucket=context.file_bucket, file_key=context.file_key,
                                     file_version_id=context.file_version_id)

    def _tag_file_with_quality_score(self, context: ProcessContext, check_result: CheckResult):
        results = check_result.rule_results
        score_summary = [f"{rule_result.rule_name}={rule_result.score}" for rule_result in results if
                         rule_result.score is not None]
        score_summary_str = ":".join(score_summary)
        status_summary = [f"{rule_result.rule_name}={rule_result.status}" for rule_result in results]
        status_summary_str = ":".join(status_summary)
        tags = {TagKeys.QualityScore.value: check_result.overall_score,
                TagKeys.QualityScoreSummary.value: score_summary_str,
                TagKeys.QualityStatusSummary.value: status_summary_str}

        self._process_steps.tag_file(tags, file_key=context.file_key, file_bucket=context.file_bucket,
                                     file_version_id=context.file_version_id)

    def _create_detailed_error_report(self, context: ProcessContext, check_result: CheckResult):
        error_dataframes = [result.errors_df for result in check_result.rule_results]

        error_key = self._process_steps.get_error_key(
            context.file_key,
            self.config.filename_timestamp_fmt,
            ErrorReportLevel.DETAILED)
        df = self._process_steps.write_detailed_error_report(
            data_frames=error_dataframes,
            bucket=context.target_bucket,
            key=error_key)
        return self._detect_and_update_schema_changes(
            context=context, df=df, col='row_index', crawler=context.error_details_crawler, reporting_level=ErrorReportLevel.DETAILED)

    def _detect_and_update_schema_changes(
            self,
            context: ProcessContext,
            df: pd.DataFrame,
            col: Optional[str],
            crawler: str,
            reporting_level: str):
        count = len(df.index)
        if count:
            partition_values = self._process_steps.extract_detail_partition_values(
                context.file_key,
                self.config.filename_timestamp_fmt)
            table_columns = list(df.columns)
            business_area, _, _ = self._process_steps.extract_business_details(
                context.file_key)
            table_name = self._process_steps.error_table_name(business_area, reporting_level)

            LOGGER.info(f"Checking error schema status with partition values {partition_values} and "
                        f"columns {table_columns}")
            needs_update = self.pipeline_catalog.is_table_needing_updating_schema(CatalogDatabase.Curated.value,
                                                                                  table_name,
                                                                                  table_columns,
                                                                                  partition_values)
            if needs_update:
                LOGGER.info("Schema changes detected, will run the schema crawler")
                start_crawler(
                    session=self._session,
                    crawler_name=crawler)
            else:
                LOGGER.info("No schema change detected, will not run the schema crawler")
        if col:
            return df[[col]]

    def _create_summary_error_report(self, context: ProcessContext, check_result: CheckResult):
        summary_df = check_result.summary_df

        error_key = self._process_steps.get_error_key(
            context.file_key,
            self.config.filename_timestamp_fmt,
            ErrorReportLevel.SUMMARY)

        written_summary_df = self._process_steps.write_summary_error_report(
            summary_df=summary_df,
            bucket=context.target_bucket,
            key=error_key,
            config=self.config)

        self._detect_and_update_schema_changes(
            context=context,
            df=written_summary_df,
            col=None,
            crawler=context.error_summary_crawler,
            reporting_level=ErrorReportLevel.SUMMARY)

    def _tag_file_with_failed(self, context: ProcessContext):
        self._process_steps.tag_file(tags={TagKeys.Status.value: TagValues.Failed.value},
                                     file_bucket=context.file_bucket, file_key=context.file_key,
                                     file_version_id=context.file_version_id)

    def _copy_from_raw_to_to_be_processed(self, context: ProcessContext, target_key: str):
        LOGGER.debug("Starting copy from raw to be processed")
        file_operations_context = self._get_file_operations_context(file_bucket=context.file_bucket,
                                                                    file_key=context.file_key,
                                                                    file_version_id=context.file_version_id,
                                                                    target_bucket=context.file_bucket,
                                                                    target_key=target_key)
        self._process_steps.copy_file(file_operations_context)
        version_id = self._process_steps.get_version_id(
            context.file_bucket, target_key)
        context.file_version_id = version_id
        context.file_key = target_key
        LOGGER.debug(f"Finished copy from raw to be processed {target_key}")

    def _move_from_to_be_processed_to_error(self, context: ProcessContext, target_key: str):
        file_operations_context = self._get_file_operations_context(file_bucket=context.file_bucket,
                                                                    file_key=context.file_key,
                                                                    target_bucket=context.file_bucket,
                                                                    target_key=target_key,
                                                                    file_version_id=context.file_version_id)
        self._process_steps.move_file(file_operations_context)
        version_id = self._process_steps.get_version_id(
            context.file_bucket, target_key)
        context.file_version_id = version_id
        context.file_key = target_key

    def _move_file_from_to_be_processed_to_processed(self, context: ProcessContext, target_key: str):
        LOGGER.debug("Moving file to processed")
        file_operations_context = self._get_file_operations_context(file_bucket=context.file_bucket,
                                                                    file_key=context.file_key,
                                                                    target_bucket=context.file_bucket,
                                                                    target_key=target_key,
                                                                    file_version_id=context.file_version_id)
        self._process_steps.move_file(file_operations_context)
        version_id = self._process_steps.get_version_id(
            context.file_bucket, target_key)
        context.file_version_id = version_id
        context.file_key = target_key

    def _find_target_key(self, context: ProcessContext, fmt: str):
        return self._process_steps.get_target_key(context.file_key, fmt)

    def _copy_file_from_processed_to_target(self, context: ProcessContext, target_key):
        file_operations_context = self._get_file_operations_context(file_bucket=context.file_bucket,
                                                                    file_key=context.file_key,
                                                                    file_version_id=context.file_version_id,
                                                                    target_bucket=context.target_bucket,
                                                                    target_key=target_key)
        self._process_steps.copy_file(file_operations_context)

    def _send_success_notification(self, context: ProcessContext, target_key):
        msg = SUCCESS_MSG.format(business_process=context.business_process, pipeline_name=context.pipeline_name,
                                 uri=URI.format(s3_aws_service=AwsService.S3.value,
                                                file_bucket=context.target_bucket, file_key=target_key))
        self._process_steps.send_notification(context, Topic.Success, AwsRegion.EUIreland, SnsStatus.Success,
                                              SUCCESS_SUBJECT.format(
                                                  business_process=context.business_process), msg,
                                              context.business_email)

    def _send_pipeline_error_notification(self, context: ProcessContext):
        subject = ERROR_SUBJECT.format(
            business_process=context.business_process)
        msg = ERROR_MSG.format(business_process=context.business_process, pipeline_name=context.pipeline_name,
                               uri=URI.format(s3_aws_service=AwsService.S3.value, file_bucket=context.file_bucket,
                                              file_key=context.file_key))
        self._send_error_notification(context, subject, msg)

    def _send_unknown_error_notification(self,
                                         context: ProcessContext,
                                         exception: Exception):
        msg = UNKNOWN_ERROR_MSG.format(business_process=context.business_process, pipeline_name=context.pipeline_name,
                                       uri=URI.format(s3_aws_service=AwsService.S3.value,
                                                      file_bucket=context.file_bucket,
                                                      file_key=context.file_key),
                                       exception=Processor._exception_description(exception),
                                       stacktrace=Processor._exception_stacktrace(exception))
        subject = ERROR_SUBJECT.format(
            business_process=context.business_process)
        self._send_error_notification(context, subject, msg)

    def _send_rule_error_notification(self, context: ProcessContext, rule_errors: List[RuleResult]):
        msgs = [f"Rule: {rule_error.rule_name}\n"
                f"Error:{rule_error.exception}\n\n"
                f"Details:{self._exception_stacktrace(rule_error.exception)}"
                for rule_error in rule_errors]
        subject = RULE_ERROR_SUBJECT.format(business_process=context.business_process)
        msg = RULE_ERROR_MSG.format(pipeline_name=context.pipeline_name, file=context.file_key, rules="\n\n".join(msgs))
        self._send_error_notification(context, subject, msg)

    def _send_parquet_catalog_error_notification(self,
                                                 context: ProcessContext,
                                                 exception: Exception):
        subject = ERROR_SUBJECT.format(
            business_process=context.business_process)
        msg = CATALOG_ERROR_MSG.format(business_process=context.business_process,
                                       pipeline_name=context.pipeline_name,
                                       uri=URI.format(s3_aws_service=AwsService.S3.value,
                                                      file_bucket=context.file_bucket,
                                                      file_key=context.file_key),
                                       exception=Processor._exception_description(exception),
                                       stacktrace=Processor._exception_stacktrace(exception))
        self._send_error_notification(context, subject, msg)

    def _send_error_notification(self, context, subject: str, msg: str):
        self._process_steps.send_notification(context, Topic.Error, AwsRegion.EUIreland, SnsStatus.Failed, subject, msg,
                                              context.support_email)

    def _write_parquet_file(self, context: ProcessContext, target_key, error_indexes):
        pipeline_parquet = PipelineParquet(self._session)
        return pipeline_parquet.create_parquet(
            context.target_bucket,
            target_key=target_key,
            config=self.config,
            error_indexes=error_indexes,
            correlation_id=context.correlation_id)

    def _delete_parquet_file(self, target_bucket, parquet_key):
        if parquet_key:
            version_id = self._process_steps.get_version_id(
                target_bucket, parquet_key)
            file_context = self._get_file_operations_context(file_key=parquet_key, file_bucket=target_bucket,
                                                             file_version_id=version_id)
            self._process_steps.delete_file(file_context)

    @staticmethod
    def _get_file_operations_context(file_bucket: str, file_key: str, file_version_id: str = None,
                                     target_bucket: str = None,
                                     target_key: str = None):
        return FileOperationsContext(file_key=file_key, file_bucket=file_bucket, file_version_id=file_version_id,
                                     target_key=target_key, target_bucket=target_bucket)

    @staticmethod
    def _exception_stacktrace(exception: Exception) -> str:
        stacktrace = extract_tb(exception.__traceback__)
        pretty = format_list(stacktrace)
        return ''.join(pretty)

    @staticmethod
    def _exception_description(exception: Exception) -> str:
        return f"{exception.__class__.__name__}: {str(exception)}"
