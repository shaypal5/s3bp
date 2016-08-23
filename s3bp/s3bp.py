"""This package enables saving and loading of pandas DataFrame objects to disk
while also backing to S3 storage. """


import os
import datetime
import ntpath  # to extract file name from path, OS-independent
import traceback  # for printing full stacktraces of errors
import concurrent.futures  # for asynchronous file uploads

try:  # for automatic caching of return values of functions
    from functools import lru_cache
except ImportError:
    from functools32 import lru_cache  # pylint: disable=E0401

import pandas as pd
import boto3  # to interact with AWS S3
from botocore.exceptions import ClientError
import dateutil  # to make local change-time datetime objects time-aware
import yaml  # to read the s3bp config


CFG_FILE_NAME = 's3bp_cfg.yml'
DEFAULT_MAX_WORKERS = 5
EXECUTOR = None


# === Reading configuration ===

@lru_cache(maxsize=2)
def _s3bp_cfg_file_path():
    return os.path.abspath(os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        CFG_FILE_NAME))


@lru_cache(maxsize=2)
def _get_s3bp_cfg():
    try:
        with open(_s3bp_cfg_file_path(), 'r') as cfg_file:
            cfg = yaml.safe_load(cfg_file)
            if not isinstance(cfg, dict):
                cfg = {'base_folder_to_bucket_map': {}},
            return cfg
    except FileNotFoundError:
        with open(_s3bp_cfg_file_path(), 'w') as outfile:
            outfile.write(yaml.dump(
                {'base_folder_to_bucket_map': {}},
                default_flow_style=False
            ))
        return _get_s3bp_cfg()


@lru_cache(maxsize=2)
def _max_workers():
    try:
        return _get_s3bp_cfg()['max_workers']
    except KeyError:
        return DEFAULT_MAX_WORKERS


@lru_cache(maxsize=2)
def _default_bucket():
    return _get_s3bp_cfg()['default_bucket']


@lru_cache(maxsize=2)
def _base_folder_to_bucket_map():
    return _get_s3bp_cfg()['base_folder_to_bucket_map']


@lru_cache(maxsize=2)
def _base_folders():
    return list(_get_s3bp_cfg()['base_folder_to_bucket_map'].keys())


# === Setting configuration ===

def _set_s3bp_cfg(cfg):
    with open(_s3bp_cfg_file_path(), 'w') as outfile:
        outfile.write(yaml.dump(cfg, default_flow_style=False))
    _get_s3bp_cfg.cache_clear()
    _default_bucket.cache_clear()
    _base_folder_to_bucket_map.cache_clear()
    _base_folders.cache_clear()
    _get_base_folder_by_file_path_and_bucket_name.cache_clear()
    _get_bucket_and_key.cache_clear()


def set_max_workers(max_workers):
    """Sets the maximum number of workers in the thread pool used to
    asynchronously upload files. NOTE: Resets the current thread pool!"""
    cfg = _get_s3bp_cfg()
    cfg['max_workers'] = max_workers
    _set_s3bp_cfg(cfg)
    _get_executor(reset=True)


def set_default_bucket(bucket_name):
    """Sets the given string as the default bucket name."""
    cfg = _get_s3bp_cfg()
    cfg['default_bucket'] = bucket_name
    _set_s3bp_cfg(cfg)


def unset_default_bucket():
    """Unsets the currently set default bucket, if set."""
    cfg = _get_s3bp_cfg()
    cfg.pop('default_bucket', None)
    _set_s3bp_cfg(cfg)


def set_default_base_folder(base_folder):
    """Sets the given string as the default base folder name."""
    cfg = _get_s3bp_cfg()
    cfg['default_base_folder'] = base_folder
    _set_s3bp_cfg(cfg)


def add_base_folder_to_bucket_mapping(base_folder, bucket_name):
    """Maps the given folder as a base folder of the given bucket.

    Arguments
    ---------
    base_folder : str
        The full path, from root, to the desired base folder.
    bucket_name : str
        The name of the bucket to map the given folder to.
    """
    cfg = _get_s3bp_cfg()
    if not isinstance(cfg['base_folder_to_bucket_map'], dict):
        cfg['base_folder_to_bucket_map'] = {}
    cfg['base_folder_to_bucket_map'][base_folder] = bucket_name
    _set_s3bp_cfg(cfg)


def remove_base_folder_mapping(base_folder):
    """Remove the mapping associated with the given folder, if exists."""
    cfg = _get_s3bp_cfg()
    cfg['base_folder_to_bucket_map'].pop(base_folder, None)
    _set_s3bp_cfg(cfg)


# === Getting parameters ===


def _get_executor(reset=False):
    if reset:
        _get_executor.executor = concurrent.futures.ThreadPoolExecutor(
            _max_workers())
    try:
        return _get_executor.executor
    except AttributeError:
        _get_executor.executor = concurrent.futures.ThreadPoolExecutor(
            _max_workers())
        return _get_executor.executor


@lru_cache(maxsize=32)
def _get_bucket_by_name(bucket_name):
    s3_rsc = boto3.resource('s3')
    return s3_rsc.Bucket(bucket_name)


@lru_cache(maxsize=32)
def _get_base_folder_by_file_path_and_bucket_name(filepath, bucket_name):
    try:
        for folder in _base_folders():
            if (folder in filepath) and (
                    _base_folder_to_bucket_map()[folder] == bucket_name):
                return folder
    except (KeyError, AttributeError):
        return None
    return None


def _bucket_name_and_base_folder_by_filepath(filepath):
    try:
        for folder in _base_folders():
            if folder in filepath:
                return _base_folder_to_bucket_map()[folder], folder
    except (KeyError, AttributeError):
        pass
    try:
        return _default_bucket(), None
    except KeyError:
        raise ValueError(
            "No bucket name was given, and neither a default was defined "
            "nor could one be interpreted from the file path. Please "
            "provide one explicitly, or define an appropriate bucket.")
    return None, None


def _get_key(filepath, namekey, base_folder):
    if namekey or not base_folder:
        return ntpath.basename(filepath)
    index = filepath.find(base_folder[base_folder.rfind('/'):])
    return filepath[index + 1:]


@lru_cache(maxsize=32)
def _get_bucket_and_key(filepath, bucket_name, namekey):
    if bucket_name is None:
        bucket_name, base_folder = _bucket_name_and_base_folder_by_filepath(
            filepath)
    elif not namekey:
        base_folder = _get_base_folder_by_file_path_and_bucket_name(
            filepath, bucket_name)
    bucket = _get_bucket_by_name(bucket_name)
    key = _get_key(filepath, namekey, base_folder)
    return bucket, key


# === Saving/loading files ===

def upload_file(filepath, bucket_name=None, namekey=None, wait=False):
    """Uploads the given file to S3 storage.

    Arguments
    ---------
    filepath : str
        The full path, from root, to the desired file.
    bucket_name (optional) : str
        The name of the bucket to upload the file to. If not given, it will be
        inferred from any defined base folder that is present on the path
        (there is no guarentee which base folder will be used if several are
        present in the given path). If base folder inferrence fails the default
        bukcet will be used, if defined, else the operation will fail.
    namekey (optional) : bool
        Indicate whether to use the name of the file as the key when uploading
        to the bucket. If set, or if no base folder is found in the filepath,
        the file name will be used as key. Otherwise, the path rooted at the
        detected base folder will be used, resulting in a folder-like structure
        in the S3 bucket.
    wait (optional) : bool
        Defaults to False. If set to True, the function will wait on the upload
        operation. Otherwise, the upload will be performed asynchronously in a
        separate thread.
    """
    bucket, key = _get_bucket_and_key(filepath, bucket_name, namekey)
    if wait:
        bucket.upload_file(filepath, key)
    else:
        _get_executor().submit(bucket.upload_file, filepath, key)


def _file_time_modified(filepath):
    timestamp = os.path.getmtime(filepath)
    dt_obj = datetime.datetime.utcfromtimestamp(timestamp)
    # this is correct only because the non-time-aware obj is in UTC!
    dt_obj = dt_obj.replace(tzinfo=dateutil.tz.tzutc())
    return dt_obj


def download_file(filepath, bucket_name=None, namekey=None, verbose=False):
    """Downloads the most recent version of the given file from S3, if needed.

    Arguments
    ---------
    filepath : str
        The full path, from root, to the desired file.
    bucket_name (optional) : str
        The name of the bucket to download the file from. If not given, it
        will be inferred from any defined base folder that is present on the
        path (there is no guarentee which base folder will be used if several
        are present in the given path). If base folder inferrence fails the
        default bukcet will be used, if defined, else the operation will fail.
    namekey (optional) : bool
        Indicate whether to use the name of the file as the key when
        downloading from the bucket. If set, or if no base folder is found in
        the filepath, the file name will be used as key. Otherwise, the path
        rooted at the detected base folder will be used, resulting in a
        folder-like structure in the S3 bucket.
    verbose (optional) : bool
        Defaults to False. If set to True, some informative messages will be
        printed.
    """
    bucket, key = _get_bucket_and_key(filepath, bucket_name, namekey)
    try:
        if os.path.isfile(filepath):
            if verbose:
                print('File %s found on disk.' % key)
            # this datetime object has tzinfo=dateutil.tz.utc()
            s3_last_modified = bucket.Object(key).get()['LastModified']
            if s3_last_modified > _file_time_modified(filepath):
                if verbose:
                    print('But S3 has an updated version. Downloading...')
                bucket.download_file(key, filepath)
        else:
            if verbose:
                print('File %s NOT found on disk. Downloading...' % key)
            bucket.download_file(key, filepath)
    except ClientError:
        if verbose:
            print('Loading dataframe failed with the following exception:')
            print(traceback.format_exc())
        raise ValueError('No dataframe found with key %s' % key)


# === Saving/loading dataframes ===

def save_dataframe(df, filepath, bucket_name=None, namekey=None, wait=False):
    """Writes the given dataframe as a CSV file to disk and S3 storage.

    Arguments
    ---------
    df : pandas.Dataframe
        The pandas dataframe object to save.
    filepath : str
        The full path, from root, to the desired file.
    bucket_name (optional) : str
        The name of the bucket to upload the file to. If not given, it will be
        inferred from any defined base folder that is present on the path
        (there is no guarentee which base folder will be used if several are
        present in the given path). If base folder inferrence fails the default
        bukcet will be used, if defined, else the operation will fail.
    namekey (optional) : bool
        Indicate whether to use the name of the file as the key when uploading
        to the bucket. If set, or if no base folder is found in the filepath,
        the file name will be used as key. Otherwise, the path rooted at the
        detected base folder will be used, resulting in a folder-like structure
        in the S3 bucket.
    wait (optional) : bool
        Defaults to False. If set to True, the function will wait on the upload
        operation. Otherwise, the upload will be performed asynchronously in a
        separate thread.
    """
    df.to_csv(filepath)
    upload_file(filepath, bucket_name, namekey, wait)


def load_dataframe(filepath, bucket_name=None, namekey=None, verbose=False):
    """Loads the most updated version of a dataframe from a CSV file, fetching
    it from S3 storage if necessary.

    Arguments
    ---------
    df : pandas.Dataframe
        The pandas dataframe object to save.
    filepath : str
        The full path, from root, to the desired file.
    bucket_name (optional) : str
        The name of the bucket to download the file from. If not given, it
        will be inferred from any defined base folder that is present on the
        path (there is no guarentee which base folder will be used if several
        are present in the given path). If base folder inferrence fails the
        default bukcet will be used, if defined, else the operation will fail.
    namekey (optional) : bool
        Indicate whether to use the name of the file as the key when
        downloading from the bucket. If set, or if no base folder is found in
        the filepath, the file name will be used as key. Otherwise, the path
        rooted at the detected base folder will be used, resulting in a
        folder-like structure in the S3 bucket.
    verbose (optional) : bool
        Defaults to False. If set to True, some informative messages will be
        printed.
    """
    download_file(filepath, bucket_name, namekey, verbose)
    return pd.read_csv(filepath)
