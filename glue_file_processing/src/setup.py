import os
from pathlib import Path

from setuptools import setup, find_packages

CURRENT_DIRECTORY = Path(__file__).parent

with open(os.path.join(CURRENT_DIRECTORY, "README.md"), "r") as fh:
    long_description = fh.read()

setup(
    name="icg-dp-glue-file-processing",
    version="1.0.0",
    author="Data Team",
    author_email="IT-DPT-Team@icgam.com",
    description="main glue job",
    long_description=long_description,
    packages=find_packages(exclude=["sample", "tests", "docs"]),
    python_requires=">=3.6",
    install_requires=['pandas==0.25.3', 's3fs==0.4.0', 'boto3==1.9.203', 'botocore==1.12.232', 'pyarrow==0.15.1',
                      'dataclasses==0.7', 'python-json-logger==0.1.11', 'icg-dp-core==6.0.40',
                      'icg-dp-data-quality==6.0.40', 'icg-dp-file-operations==6.0.40', 'icg-dp-glue-catalog==6.0.40',
                      'icg-dp-serializer==6.0.40', 'icg-dp-hashing==6.0.40', 'icg-dp-loggings==6.0.40',
                      'icg-dp-sns==6.0.40', 'icg-dp-tagger==6.0.40']
)
