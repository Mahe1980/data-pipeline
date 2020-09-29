from pathlib import Path

from .pipeline_enum import PipelineFolders, FileSuffixPrefix


class PipelineProcessFolder:

    def __init__(self, file_key: str, processed_on: str):
        self._file_key = file_key
        self._root_folder = file_key[:file_key.find(
            PipelineFolders.Automated.value) - 1]
        self._processed_on = processed_on
        self._file_name = file_key.split('/')[-1]

    @property
    def to_be_processed(self):
        return f"{self._root_folder}/{PipelineFolders.ToBeProcessed.value}/{self._file_name}"

    @property
    def processed(self):
        return f"{self._root_folder}/{PipelineFolders.Processed.value}/{self._get_timestamp_folder_structure()}/" \
            f"{self._file_name}"

    @property
    def error(self):
        return f"{self._root_folder}/{PipelineFolders.ProcessFailed.value}/" \
            f"{self._get_timestamp_folder_structure()}/{self._file_name}"

    @property
    def error_file(self):
        return f"{self._root_folder}/{PipelineFolders.ProcessFailed.value}/" \
            f"{self._get_timestamp_folder_structure()}/" \
            f"{FileSuffixPrefix.ErrorPrefix.value}{Path(self._file_name).stem}{FileSuffixPrefix.ErrorExtension.value}"

    def _get_timestamp_folder_structure(self):
        year, month = self._processed_on.split('-')[:2]
        return f"{year}/{year}{month}"
