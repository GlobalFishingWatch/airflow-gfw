#!/usr/bin/env python

"""
Setup script for airflow-gfw
"""

import codecs
import os

from setuptools import find_packages
from setuptools import setup


DEPENDENCIES = [
    "apache-airflow==1.10.14",
    "google-api-python-client",
    "httplib2",
    "pytest",
    "pytz"
]

AIRFLOW_DEPENDENCIES = [
    "marshmallow==2.21.0"
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
