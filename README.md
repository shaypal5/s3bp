# s3bp = S3-backed pandas

TL;DR: Read and write pandas' DataFrames from/to S3, caching them on your hard drive to avoid unnecessary IO.

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
