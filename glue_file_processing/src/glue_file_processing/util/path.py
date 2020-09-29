import re
from datetime import datetime


def get_root_folder(file_key: str):
    root_folder = '/'.join(file_key.split('/')[:3])
    return root_folder


def get_file_name(file_key: str, include_extension: bool = True) -> str:
    file_name = file_key.split('/')[-1]
    return file_name if include_extension else file_name.split(".")[0]


def get_filename_timestamp(filename: str, filename_timestamp_fmt: str) -> datetime:
    try:
        match = re.search(r'\d{4}\d{2}\d{2}\d{2}\d{2}\d{2}', get_file_name(filename))
        return datetime.strptime(match.group(), filename_timestamp_fmt)
    except Exception as ex:
        raise ValueError(
            f'Unable to parse timestamp from file name. '
            f'Expected format is "{filename_timestamp_fmt}" and the file name is "{filename}"') from ex
