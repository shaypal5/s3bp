"""Setup file for the s3bp package."""

# This file is part of s3bp.
# https://github.com/shaypal5/s3bp

# Licensed under the MIT license:
# http://www.opensource.org/licenses/MIT-license
# Copyright (c) 2016, Shay Palachy <shaypal5@gmail.com>

from setuptools import setup, find_packages
import versioneer

setup(
    name='s3bp',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description='Read and write Python objects from/to S3.',
    license='MIT',
    author='Shay Palachy',
    author_email='shaypal5@gmail.com',
    url='https://github.com/shaypal5/s3bp',
    # download_url='https://github.com/shaypal5/s3po/tarball/0.1.1',
    packages=find_packages(),
    install_requires=[
        'botocore',
        'boto3',
        'python-dateutil',
        'pyyaml',
        'pandas',
        'feather-format'
    ],
    keywords=['pandas', 'dataframe', 's3'],
    classifiers=[],
)
