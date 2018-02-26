"""This package enables saving and loading of python objects to disk
while also backing to S3 storage. """


import os
import datetime
import ntpath  # to extract file name from path, OS-independent
import traceback  # for printing full stacktraces of errors
import concurrent.futures  # for asynchronous file uploads
import pickle  # for pickling files

try:  # for automatic caching of return values of functions
    from functools import lru_cache
except ImportError:
    from functools32 import lru_cache  # pylint: disable=E0401

import pandas as pd
import boto3  # to interact with AWS S3
from botocore.exceptions import ClientError
import dateutil  # to make local change-time datetime objects time-aware
import yaml  # to read the s3bp config
import feather  # to read/write pandas dataframes as feather objects


CFG_FILE_NAME = 's3bp_cfg.yml'
DEFAULT_MAX_WORKERS = 5
EXECUTOR = None


# === Reading configuration ===

def _s3bp_cfg_file_path():
    return os.path.abspath(os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        CFG_FILE_NAME))


def _get_s3bp_cfg():
    try:
        with open(_s3bp_cfg_file_path(), 'r') as cfg_file:
            cfg = yaml.safe_load(cfg_file)
            if not isinstance(cfg, dict):
                cfg = {'base_dir_to_bucket_map': {}},
            return cfg
    except FileNotFoundError:
        with open(_s3bp_cfg_file_path(), 'w') as outfile:
            outfile.write(yaml.dump(
                {'base_dir_to_bucket_map': {}},
                default_flow_style=False
            ))
        return _get_s3bp_cfg()


def _max_workers():
    try:
        return _get_s3bp_cfg()['max_workers']
    except KeyError:
        return DEFAULT_MAX_WORKERS


def _default_bucket():
    return _get_s3bp_cfg()['default_bucket']


def _base_dir_to_bucket_map():
    return _get_s3bp_cfg()['base_dir_to_bucket_map']


def _base_dirs():
    return list(_get_s3bp_cfg()['base_dir_to_bucket_map'].keys())


# === Setting configuration ===

def _set_s3bp_cfg(cfg):
    with open(_s3bp_cfg_file_path(), 'w') as outfile:
        outfile.write(yaml.dump(cfg, default_flow_style=False))


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


def _parse_dir_path(dir_path):
    if '~' in dir_path:
        return os.path.expanduser(dir_path)
    return dir_path


def set_default_base_directory(base_directory):
    """Sets the given string as the default base directory name."""
    cfg = _get_s3bp_cfg()
    cfg['default_base_dir'] = _parse_dir_path(base_directory)
    _set_s3bp_cfg(cfg)


def map_base_directory_to_bucket(base_directory, bucket_name):
    """Maps the given directory as a base directory of the given bucket.

    Arguments
    ---------
    base_directory : str
        The full path, from root, to the desired base directory.
    bucket_name : str
        The name of the bucket to map the given directory to.
    """
    cfg = _get_s3bp_cfg()
    parsed_path = _parse_dir_path(base_directory)
    if not isinstance(cfg['base_dir_to_bucket_map'], dict):
        cfg['base_dir_to_bucket_map'] = {}
    cfg['base_dir_to_bucket_map'][parsed_path] = bucket_name
    _set_s3bp_cfg(cfg)


def remove_base_directory_mapping(base_directory):
    """Remove the mapping associated with the given directory, if exists."""
    cfg = _get_s3bp_cfg()
    parsed_path = _parse_dir_path(base_directory)
    cfg['base_dir_to_bucket_map'].pop(parsed_path, None)
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
def _get_base_dir_by_file_path_and_bucket_name(filepath, bucket_name):
    try:
        for directory in _base_dirs():
            if (directory in filepath) and (
                    _base_dir_to_bucket_map()[directory] == bucket_name):
                return directory
    except (KeyError, AttributeError):
        return None
    return None


def _bucket_name_and_base_dir_by_filepath(filepath):
    try:
        for directory in _base_dirs():
            if directory in filepath:
                return _base_dir_to_bucket_map()[directory], directory
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


def _get_key(filepath, namekey, base_directory):
    if namekey or not base_directory:
        return ntpath.basename(filepath)
    index = filepath.find(base_directory[base_directory.rfind('/'):])
    return filepath[index + 1:]


@lru_cache(maxsize=32)
def _get_bucket_and_key(filepath, bucket_name, namekey):
    base_directory = None
    if bucket_name is None:
        bucket_name, base_directory = _bucket_name_and_base_dir_by_filepath(
            filepath)
    elif not namekey:
        base_directory = _get_base_dir_by_file_path_and_bucket_name(
            filepath, bucket_name)
        os.makedirs(base_directory, exist_ok=True)
    bucket = _get_bucket_by_name(bucket_name)
    key = _get_key(filepath, namekey, base_directory)
    return bucket, key


# === Uploading/Downloading files ===

def _parse_file_path(filepath):
    if '~' in filepath:
        return os.path.expanduser(filepath)
    return filepath


def _file_upload_thread(bucket, filepath, key):
    try:
        bucket.upload_file(filepath, key)
    except BaseException as exc:  # pylint: disable=W0703
        print(
            'File upload failed with following exception:\n{}'.format(exc),
            flush=True
        )


def upload_file(filepath, bucket_name=None, namekey=None, wait=False):
    """Uploads the given file to S3 storage.

    Arguments
    ---------
    filepath : str
        The full path, from root, to the desired file.
    bucket_name (optional) : str
        The name of the bucket to upload the file to. If not given, it will be
        inferred from any defined base directory that is present on the path
        (there is no guarentee which base directory will be used if several are
        present in the given path). If base directory inferrence fails the
        default bukcet will be used, if defined, else the operation will fail.
    namekey (optional) : bool
        Indicate whether to use the name of the file as the key when uploading
        to the bucket. If set, or if no base directory is found in the
        filepath, the file name will be used as key. Otherwise, the path
        rooted at the detected base directory will be used, resulting in a
        directory-like structure in the S3 bucket.
    wait (optional) : bool
        Defaults to False. If set to True, the function will wait on the upload
        operation. Otherwise, the upload will be performed asynchronously in a
        separate thread.
    """
    filepath = _parse_file_path(filepath)
    bucket, key = _get_bucket_and_key(filepath, bucket_name, namekey)
    if wait:
        bucket.upload_file(filepath, key)
    else:
        _get_executor().submit(_file_upload_thread, bucket, filepath, key)


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
        will be inferred from any defined base directory that is present on
        the path (there is no guarentee which base directory will be used if
        several are present in the given path). If base directory inferrence
        fails the default bukcet will be used, if defined, else the operation
        will fail.
    namekey (optional) : bool
        Indicate whether to use the name of the file as the key when
        downloading from the bucket. If set, or if no base directory is found
        in the filepath, the file name will be used as key. Otherwise, the path
        rooted at the detected base directory will be used, resulting in a
        directory-like structure in the S3 bucket.
    verbose (optional) : bool
        Defaults to False. If set to True, some informative messages will be
        printed.
    """
    filepath = _parse_file_path(filepath)
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
                # creating non-existing dirs on the path
                if not os.path.exists(filepath):
                    os.makedirs(filepath[:filepath.rfind('/')])
            bucket.download_file(key, filepath)
    except ClientError:
        if verbose:
            print('Loading dataframe failed with the following exception:')
            print(traceback.format_exc())
        raise ValueError('No dataframe found with key %s' % key)


# === Saving/loading Python objects ===

def _pickle_serialiazer(pyobject, filepath):
    pickle.dump(pyobject, open(filepath, 'wb'))


def save_object(pyobject, filepath, bucket_name=None,
                serializer=_pickle_serialiazer, namekey=None, wait=False):
    """Saves the given object to S3 storage, caching it as the given file.

    Arguments
    ---------
    pyobject : object
        The python object to save.
    filepath : str
        The full path, from root, to the desired cache file.
    bucket_name (optional) : str
        The name of the bucket to upload the file to. If not given, it will be
        inferred from any defined base directory that is present on the path
        (there is no guarentee which base directory will be used if several are
        present in the given path). If base directory inferrence fails the
        default bukcet will be used, if defined, else the operation will fail.
    serializer (optional) : callable
        A callable that takes two positonal arguments, a Python object and a
        path to a file, and dumps the object to the given file. Defaults to a
        wrapper of pickle.dump.
    namekey (optional) : bool
        Indicate whether to use the name of the file as the key when uploading
        to the bucket. If set, or if no base directory is found in the
        filepath, the file name will be used as key. Otherwise, the path
        rooted at the detected base directory will be used, resulting in a
        directory-like structure in the S3 bucket.
    wait (optional) : bool
        Defaults to False. If set to True, the function will wait on the upload
        operation. Otherwise, the upload will be performed asynchronously in a
        separate thread.
    """
    serializer(pyobject, filepath)
    upload_file(filepath, bucket_name, namekey, wait)


def _picke_deserializer(filepath):
    return pickle.load(open(filepath, 'rb'))


def load_object(filepath, bucket_name=None, deserializer=_picke_deserializer,
                namekey=None, verbose=False):
    """Loads the most recent version of the object cached in the given file.

    Arguments
    ---------
    filepath : str
        The full path, from root, to the desired file.
    bucket_name (optional) : str
        The name of the bucket to download the file from. If not given, it
        will be inferred from any defined base directory that is present on
        the path (there is no guarentee which base directory will be used if
        several are present in the given path). If base directory inferrence
        fails the default bukcet will be used, if defined, else the operation
        will fail.
    deserializer (optional) : callable
        A callable that takes one positonal argument, a path to a file, and
        returns the object stored in it. Defaults to a wrapper of pickle.load.
    namekey (optional) : bool
        Indicate whether to use the name of the file as the key when
        downloading from the bucket. If set, or if no base directory is found
        in the filepath, the file name will be used as key. Otherwise, the path
        rooted at the detected base directory will be used, resulting in a
        directory-like structure in the S3 bucket.
    verbose (optional) : bool
        Defaults to False. If set to True, some informative messages will be
        printed.
    """
    download_file(filepath, bucket_name=bucket_name, namekey=namekey,
                  verbose=verbose)
    return deserializer(filepath)


# === Saving/loading dataframes ===

def _pandas_df_csv_serializer(pyobject, filepath):
    pyobject.to_csv(filepath)


def _pandas_df_excel_serializer(pyobject, filepath):
    pyobject.to_excel(filepath)


def _pandas_df_feather_serializer(pyobject, filepath):
    feather.write_dataframe(pyobject, filepath)


def _get_pandas_df_serializer(dformat):
    dformat = dformat.lower()
    if dformat == 'csv':
        return _pandas_df_csv_serializer
    if dformat == 'excel':
        return _pandas_df_excel_serializer
    if dformat == 'feather':
        return _pandas_df_feather_serializer


def save_dataframe(df, filepath, bucket_name=None, dformat='csv', namekey=None,
                   wait=False):
    """Writes the given dataframe as a CSV file to disk and S3 storage.

    Arguments
    ---------
    df : pandas.Dataframe
        The pandas Dataframe object to save.
    filepath : str
        The full path, from root, to the desired file.
    bucket_name (optional) : str
        The name of the bucket to upload the file to. If not given, it will be
        inferred from any defined base directory that is present on the path
        (there is no guarentee which base directory will be used if several are
        present in the given path). If base directory inferrence fails the
        default bukcet will be used, if defined, else the operation will fail.
    dformat (optional) : str
        The storage format for the Dataframe. One of 'csv','excel' and
        'feather'. Defaults to 'csv'.
    namekey (optional) : bool
        Indicate whether to use the name of the file as the key when uploading
        to the bucket. If set, or if no base directory is found in the
        filepath, the file name will be used as key. Otherwise, the path
        rooted at the detected base directory will be used, resulting in a
        directory-like structure in the S3 bucket.
    wait (optional) : bool
        Defaults to False. If set to True, the function will wait on the upload
        operation. Otherwise, the upload will be performed asynchronously in a
        separate thread.
    """
    save_object(df, filepath, serializer=_get_pandas_df_serializer(dformat),
                bucket_name=bucket_name, namekey=namekey, wait=wait)


def _pandas_df_csv_deserializer(filepath):
    return pd.read_csv(filepath)


def _pandas_df_excel_deserializer(filepath):
    return pd.read_excel(filepath)


def _pandas_df_feather_deserializer(filepath):
    return feather.read_dataframe(filepath)


def _get_pandf_defserializer(dformat):
    dformat = dformat.lower()
    if dformat == 'csv':
        return _pandas_df_csv_deserializer
    if dformat == 'excel':
        return _pandas_df_excel_deserializer
    if dformat == 'feather':
        return _pandas_df_feather_deserializer


def load_dataframe(filepath, bucket_name=None, dformat='csv', namekey=None,
                   verbose=False):
    """Loads the most updated version of a dataframe from file, fetching it
    from S3 storage if necessary.

    Arguments
    ---------
    filepath : str
        The full path, from root, to the desired file.
    bucket_name (optional) : str
        The name of the bucket to download the file from. If not given, it
        will be inferred from any defined base directory that is present on
        the path (there is no guarentee which base directory will be used if
        several are present in the given path). If base directory inferrence
        fails the default bukcet will be used, if defined, else the operation
        will fail.
    dformat (optional) : str
        The storage format for the Dataframe. One of 'csv','excel' and
        'feather'. Defaults to 'csv'.
    namekey (optional) : bool
        Indicate whether to use the name of the file as the key when
        downloading from the bucket. If set, or if no base directory is found
        in the filepath, the file name will be used as key. Otherwise, the path
        rooted at the detected base directory will be used, resulting in a
        directory-like structure in the S3 bucket.
    verbose (optional) : bool
        Defaults to False. If set to True, some informative messages will be
        printed.
    """
    return load_object(
        filepath, deserializer=_get_pandf_defserializer(dformat),
        bucket_name=bucket_name, namekey=namekey, verbose=verbose)
