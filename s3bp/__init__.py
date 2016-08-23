"""This package enables saving and loading of pandas DataFrame objects to disk
while also backing to S3 storage. """

from .s3bp import (
    set_max_workers,
    set_default_bucket,
    unset_default_bucket,
    set_default_base_directory,
    map_base_directory_to_bucket,
    remove_base_directory_mapping,
    upload_file,
    download_file,
    save_dataframe,
    load_dataframe
)
