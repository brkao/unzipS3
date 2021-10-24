import json
import boto3
from io import BytesIO
import gzip
import os
from pathlib import Path

def lambda_handler(event, context):
    tgt_s3_bucket = os.environ.get('TARGET_S3_BUCKET')
    access_key = os.environ.get('ACCESS_KEY')
    secret_key = os.environ.get('SECRET_KEY')
    print("Target S3 Bucket ", tgt_s3_bucket)
    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )
    s3_resource = session.resource('s3')
    s3_client = session.client('s3')

    records = [x for x in event.get('Records', []) if x.get('eventName') == 'ObjectCreated:Put']
    sorted_events = sorted(records, key=lambda e: e.get('eventTime'))
    latest_event = sorted_events[-1] if sorted_events else {}
    info = latest_event.get('s3', {})
    file_key = info.get('object', {}).get('key')
    if !file_key.endswith('.gz'):
        return

    bucket_name = info.get('bucket', {}).get('name')
    uncompressed_key = str(Path(file_key).with_suffix(''))

    print("Event for Bucket [%s] File [%s] Uncompressed [%s]" % (bucket_name, file_key, uncompressed_key))
    try:
        zip_obj = s3_resource.Object(bucket_name=bucket_name, key=file_key)
        fileobj=gzip.GzipFile(
                None,
                'rb',
                fileobj=BytesIO(s3_client.get_object(Bucket=bucket_name, Key=file_key)['Body'].read()))
        s3_client.upload_fileobj(
            fileobj,
            Bucket=tgt_s3_bucket,
            Key=uncompressed_key)
    except Exception as e:
        print(e)

