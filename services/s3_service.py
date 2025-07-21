# services/s3_service.py

import boto3
import pandas as pd
from io import StringIO
import os

class S3Service:
    def __init__(self):
        self.client = boto3.client(
            's3',
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_REGION")
        )

    def read_csv(self, bucket_name, key):
        try:
            response = self.client.get_object(Bucket=bucket_name, Key=key)
            return pd.read_csv(response["Body"])
        except Exception as e:
            raise Exception(f"Error reading CSV from S3: {e}")

    def upload_csv(self, df, bucket_name, key):
        try:
            buffer = StringIO()
            df.to_csv(buffer, index=False)
            self.client.put_object(Bucket=bucket_name, Key=key, Body=buffer.getvalue())
            return f"s3://{bucket_name}/{key}"
        except Exception as e:
            raise Exception(f"Error uploading CSV to S3: {e}")
