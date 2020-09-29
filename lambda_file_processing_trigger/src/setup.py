import os
from pathlib import Path

from setuptools import setup, find_packages

CURRENT_DIRECTORY = Path(__file__).parent

with open(os.path.join(CURRENT_DIRECTORY, "README.md"), "r") as fh:
    long_description = fh.read()

setup(
    name="icg-dp-lambda-file-processing-trigger",
    version="1.0.0",
    author="Data Team",
    author_email="IT-DPT-Team@icgam.com",
    description="Package to create aws lambda_file_processing_trigger",
    packages=find_packages(exclude=["sample", "tests", "docs"]),
    python_requires=">=3.6",
    install_requires=['python-json-logger==0.1.11', 'icg-dp-core==6.0.40', 'icg-dp-loggings==6.0.40']
)
