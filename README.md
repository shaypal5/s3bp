# s3bp = S3-backed pandas

Read and write pandas' DataFrames from/to S3, caching them (as CSVs) on your hard drive to avoid unnecessary IO.

```
import s3bp
s3bp.save_dataframe(dataset_dataframe, filepath, 'my-dataset-s3-bucket')
last_week_dataset = s3bp.load_dataframe(second_filepath, 'my-dataset-s3-bucket')
```

## Dependencies and Setup

s3bp uses the following packages:
- pandas
- boto3
- botocore (instaled with boto3)
- dateutil (a.k.a. python-dateutil)
- pyyaml

The boto3 package itself requires that you have an AWS config file at ```~/.aws/config``` with your AWS account credentials to successfully communicate with AWS. [Read here](http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html) on how you can configure it.

You can install s3bp using:
```
pip install s3bp
```

## Use

### Saving
Save a dataframe to your bucket with:
```
import s3bp
import pandas as pd
df1 = pd.Dataframe(data=[[1,3],[6,2]], columns=['A','B'], index=[1,2])
s3bp.save_dataframe(df1, '~/Desktop/datasets/weasels.csv', 'my-datasets-s3-bucket')
```
File upload is done asynchronously and in the background by default, only printing exceptions (and not throwing them). If you'd like to wait on your upload, and/or for a failed upload to raise an exception rather than print one, set ```wait=True```:
```
s3bp.save_dataframe(df1, '~/Desktop/datasets/weasels.csv', 'my-datasets-s3-bucket', wait=True)
```


### Loading
Load a dataframe from your bucket with:
```
df1 = s3bp.load_dataframe('~/Desktop/datasets/weasels.csv', 'my-datasets-s3-bucket')
```
Notice that if the most updated version is already on your hard drive, it will be loaded from disk. If, however, a more updated version is found on the S3 (determined by comparing modification time), or if the file is not present, it will be downloaded from S3. Furthermore, any missing directories on the path will be created.

### Default Bucket
You can set a default bucket with:
```
s3bp.set_default_bucket('my-datasets-s3-bucket')
```

You can now load and save frames without specifying a bucket, in which case the default bucket will be used:
```
df2 = s3bp.load_dataframe('~/Desktop/datasets/ferrets.csv')
```
Once set, your configuration will presist through sessions. If you'd like to unset the default bucket - making operations with no bucket specification fail - use ```s3bp.unset_default_bucket()```.

### Base Directories
You can set a specific directory as a base directory, mapping it to a specific bucket, using:
```
s3bp.map_base_directory_to_bucket('~/Desktop/labels', 'my-labels-s3-bucket')
```
Now, saving or loading a dataframe from a file in that directory - including sub-directories - will automatically use the mapped bucket, unless a different bucket is given explicitly. Furthermore, the CSV files uploaded to the bucket will not be keyed by their file name, but by the sub-path rotted at the given base directory.

This effectively results in replicating the directory tree rooted at this directory on the bucket. For example, given the above mapping, saving a Dataframe to the path ```~/Desktop/labels/user_generated/skunks.csv``` will also create a ```labels``` folder on the ```my-labels-s3-bucket```, a ```user_generated``` folder inside it and will upload the file into ```labels/user_generated```.

You can add as many base directories as you want, and can map several to the same bucket, or each to a different one.

This can be used both to automatocally backup entire folders (and their sub-folder structure) to S3 and to share these kind of folders over different machines reading and writing Dataframes into them at different times.
