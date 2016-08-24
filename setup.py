"""Setup file for the s3po package."""

from distutils.core import setup
import versioneer

setup(
    name='s3po',
    packages=['s3po'],
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description='Read and write Python objects from/to S3.',
    author='Shay Palachy',
    author_email='shaypal5@gmail.com',
    url='https://github.com/shaypal5/s3po',
    # download_url='https://github.com/shaypal5/s3po/tarball/0.1.1',
    keywords=['pandas', 'dataframe', 's3'],
    classifiers=[],
)
