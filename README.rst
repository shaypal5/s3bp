s3bp = S3-backed Python (objects)
=================================

Read and write Python objects from/to S3, caching them on your hard drive to avoid unnecessary IO.
Special care given to pandas dataframes.

.. code-block:: python

    import s3bp
    s3bp.save_object(name_to_id_dict, filepath, 'user-data-bucket')
    last_week_dataset = s3bp.load_object(second_filepath, 'my-dataset-s3-bucket')

Dependencies and Setup
----------------------

s3bp uses the following packages:

* boto3
* botocore (instaled with boto3)
* dateutil (a.k.a. python-dateutil)
* pyyaml
* pandas
* feather-format

The boto3 package itself requires that you have an AWS config file at ``~/.aws/config`` with your AWS account credentials to successfully communicate with AWS. [Read here](http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html) on how you can configure it.

You can install s3bp using:

.. code-block:: python

    pip install s3bp

Use
---

Saving
~~~~~~
Save an object to your bucket with:

.. code-block:: python

    import s3bp
    name_to_id_dict = {'Dan': 8382, 'Alon': 2993}
    s3bp.save_object(name_to_id_dict, '~/Documents/data_files/name_to_id_map', 'user-data-bucket')

File upload is done asynchronously and in the background by default, only printing exceptions (and not throwing them). If you'd like to wait on your upload, and/or for a failed upload to raise an exception rather than print one, set ``wait=True``:

.. code-block:: python

    s3bp.save_object(name_to_id_dict, '~/Documents/data_files/name_to_id_map', 'user-data-bucket', wait=True)

Loading
~~~~~~~
Load an object from your bucket with:

.. code-block:: python

    name_to_id_dict = s3bp.load_object('~/Documents/data_files/name_to_id_map', 'user-data-bucket')

Notice that if the most updated version is already on your hard drive, it will be loaded from disk. If, however, a more updated version is found on the S3 (determined by comparing modification time), or if the file is not present, it will be downloaded from S3. Furthermore, any missing directories on the path will be created.

Serialization Format
~~~~~~~~~~~~~~~~~~~~

Objects are saved as Python pickle files by default. You can change the way objects are serialized by providing a different serializer when calling ``save_object``. A serializer is a callable that takes two positonal arguments - a Python object and a path to a file - and dumps the object to the given file. It doesn't have to serialize all Python objects successfully.

For example:

.. code-block:: python

    def pandas_df_csv_serializer(pyobject, filepath):
        pyobject.to_csv(filepath)
    
    import pandas as pd
    df1 = pd.Dataframe(data=[[1,3],[6,2]], columns=['A','B'], index=[1,2])
    s3bp.save_object(df1, '~/Documents/data_files/my_frame.csv', 'user-data-bucket', serializer=pandas_df_csv_serializer)

Notice that a corresponding deserializer will have to be provided when loading the object by providing ``load_object`` with a deserializing callable through the ``deserializer`` keyword argument.

Default Bucket
~~~~~~~~~~~~~~
You can set a default bucket with:
.. code-block:: python
    s3bp.set_default_bucket('user-data-bucket')

You can now load and save objects without specifying a bucket, in which case the default bucket will be used:

.. code-block:: python

    profile_dict = s3bp.load_object('~/Documents/data_files/profile_map')

Once set, your configuration will presist through sessions. If you'd like to unset the default bucket - making operations with no bucket specification fail - use ``s3bp.unset_default_bucket()``.

Base Directories
~~~~~~~~~~~~~~~~
You can set a specific directory as a base directory, mapping it to a specific bucket, using:

.. code-block:: python

    s3bp.map_base_directory_to_bucket('~/Desktop/labels', 'my-labels-s3-bucket')

Now, saving or loading objects from files in that directory - including sub-directories - will automatically use the mapped bucket, unless a different bucket is given explicitly. Furthermore, the files uploaded to the bucket will not be keyed by their file name, but by the sub-path rotted at the given base directory.

This effectively results in replicating the directory tree rooted at this directory on the bucket. For example, given the above mapping, saving an object to the path ``~/Desktop/labels/user_generated/skunks.csv`` will also create a ``labels`` folder on the ``my-labels-s3-bucket``, a ``user_generated`` folder inside it and will upload the file into ``labels/user_generated``.

**You can add as many base directories as you want**, and can map several to the same bucket, or each to a different one.

This can be used both to automatocally backup entire folders (and their sub-folder structure) to S3 and to synchronize these kind of folders over different machines reading and writing Dataframes into them at different times.


Pandas love <3
--------------

Special care is given to pandas Dataframe objects, for which a couple of dedicated wrapper methods and several serializers are already defined. To save a dataframe use:

.. code-block:: python

    import s3bp
    import pandas as pd
    df1 = pd.Dataframe(data=[[1,3],[6,2]], columns=['A','B'], index=[1,2])
    s3bp.save_dataframe(df1, '~/Desktop/datasets/weasels.csv', 'my-datasets-s3-bucket')

This will use the default CSV serializer to save the dataframe to disk.
Similarly, you can load a dataframe from your bucket with:

.. code-block:: python

    df1 = s3bp.load_dataframe('~/Desktop/datasets/weasels.csv', 'my-datasets-s3-bucket')

To use another format assign the corresponding string to the ``format`` keyword:

.. code-block:: python

    s3bp.save_dataframe(df1, '~/Desktop/datasets/weasels.csv', 'my-datasets-s3-bucket', format='feather')

Suported pandas Dataframes serialization formats:

* CSV
* Excel
* Feather (see [the feather package](https://github.com/wesm/feather))
