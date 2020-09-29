from dataclasses import dataclass


@dataclass
class ProcessContext:
    correlation_id: str
    file_bucket: str
    file_key: str
    file_version_id: str
    target_bucket: str
    processed_on: str
    business_process: str
    pipeline_name: str
    business_email: str
    account_number: str
    support_email: str
    error_details_crawler: str
    error_summary_crawler: str
