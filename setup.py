"""Setup file for the s3bp package."""

from distutils.core import setup

setup(
    name='s3bp',
    packages=['s3bp'],
    version='0.1',
    description='Read and write pandas DataFrames from/to S3.',
    author='Shay Palachy',
    author_email='shaypal5@gmail.com',
    url='https://github.com/shaypal5/s3bp',
    download_url='https://github.com/shaypal5/s3bp/tarball/0.1',
    keywords=['pandas', 'dataframe', 's3'],
    classifiers=[],
)
