#!/usr/bin/env python

"""
Setup script for airflow-gfw
"""

import codecs
import os

from setuptools import find_packages
from setuptools import setup


DEPENDENCIES = [
    "apache-airflow==1.10.5",
    "cryptography",
    "funcsigs==1.0.0",
    "kubernetes==8.0.1",
    "nose",
    "pandas-gbq==0.9.0",
    "pytest",
    "python-dateutil",
    "pytz",
    "udatetime",
    "google-cloud-storage~=1.16"
]

AIRFLOW_DEPENDENCIES = [
    "google-api-python-client",
    "snakebite"
]

with codecs.open('README.md', encoding='utf-8') as f:
    readme = f.read().strip()


version = None
author = None
email = None
source = None
with open(os.path.join('airflow_ext', '__init__.py')) as f:
    for line in f:
        if line.strip().startswith('__version__'):
            version = line.split('=')[1].strip().replace(
                '"', '').replace("'", '')
        elif line.strip().startswith('__author__'):
            author = line.split('=')[1].strip().replace(
                '"', '').replace("'", '')
        elif line.strip().startswith('__email__'):
            email = line.split('=')[1].strip().replace(
                '"', '').replace("'", '')
        elif line.strip().startswith('__source__'):
            source = line.split('=')[1].strip().replace(
                '"', '').replace("'", '')
        elif None not in (version, author, email, source):
            break


setup(
    author=author,
    author_email=email,
    description="A python utility library for airflow extension",
    include_package_data=True,
    install_requires=DEPENDENCIES + AIRFLOW_DEPENDENCIES,
    keywords='AIS GIS remote sensing',
    license="Apache 2.0",
    long_description=readme,
    name='airflow-gfw',
    packages=find_packages(exclude=['test*.*', 'tests']),
    url=source,
    version=version,
    zip_safe=True
)
