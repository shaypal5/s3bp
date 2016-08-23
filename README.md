# s3bp = S3-backed pandas

TL;DR: Read and write pandas' DataFrames from/to S3, caching them (as CSV files) on your hard drive to avoid unnecessary IO.

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

The boto3 package itself requires that you have an AWS config file at ```~/.aws/config``` with your AWS account credentials to successfully communicate with AWS.

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
s3bp.save_dataframe(df1, '~/Desktop/datasets/ferrets.csv', 'my-datasets-s3-bucket')
```
File upload is done asynchronously and in the background by default, only printing exceptions (and not throwing them). If you'd like to wait on your upload, and/or for a failed upload to raise an exception rather than print one, set ```wait=True```:
```
s3bp.save_dataframe(df1, '~/Desktop/datasets/ferrets.csv', 'my-datasets-s3-bucket', wait=True)
```


### Loading
Load a dataframe from your bucket with:
```
df2 = s3bp.load_dataframe('~/Desktop/datasets/ferrets.csv', 'my-datasets-s3-bucket')
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
