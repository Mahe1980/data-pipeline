from enum import Enum


class TagValues(Enum):
    Failed = 'failed'
    Processed = 'processed'


class TagKeys(Enum):
    Hash = 'hash'
    QualityScore = 'overall_quality_score'
    Status = 'status'
    CorrelationId = 'correlation_id'
    QualityScoreSummary = 'quality_score_summary'
    QualityStatusSummary = 'quality_status_summary'


class SnsStatus(Enum):
    Failed = 'Failed'
    Success = 'Success'


class SnsMessageType(Enum):
    Html = 'html'
    Txt = 'txt'


class PipelineFolders(Enum):
    ToBeProcessed = "AutomatedStatus/ToBeProcessed"
    Processed = "AutomatedStatus/Processed"
    ProcessFailed = "AutomatedStatus/ProcessFailed"
    Automated = "Automated"


class FileSuffixPrefix(Enum):
    ErrorExtension = '.txt'
    ErrorPrefix = 'error_'


class Encoding(Enum):
    Utf8 = 'utf-8'


class S3Object(Enum):
    VersionId = 'VersionId'


class CatalogDatabase(Enum):
    Curated = "curated"
