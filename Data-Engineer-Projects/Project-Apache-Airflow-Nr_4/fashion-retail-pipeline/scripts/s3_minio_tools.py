import boto3
import os
from pathlib import Path
from botocore.client import Config


def get_s3_client():
    """Get S3 client configured for MinIO"""
    return boto3.client(
        's3',
        endpoint_url=os.getenv('S3_ENDPOINT', 'http://minio:9000'),
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID', 'minioadmin'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY', 'minioadmin'),
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )


def create_bucket_if_not_exists(bucket_name):
    """Create S3 bucket if it doesn't exist"""
    s3 = get_s3_client()
    try:
        s3.head_bucket(Bucket=bucket_name)
    except s3.exceptions.NoSuchBucket:
        s3.create_bucket(Bucket=bucket_name)
        print(f"Created bucket: {bucket_name}")
    except Exception as e:
        print(f"Error checking bucket: {e}")


def upload_file_to_s3(local_path, bucket, s3_key):
    """Upload file to S3/MinIO"""
    s3 = get_s3_client()
    s3.upload_file(local_path, bucket, s3_key)
    print(f"Uploaded {local_path} to s3://{bucket}/{s3_key}")


def list_files_in_bucket(bucket, prefix=''):
    """List files in bucket with optional prefix"""
    s3 = get_s3_client()
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    files = []
    if 'Contents' in response:
        for obj in response['Contents']:
            files.append(obj['Key'])
    return files


def wait_for_file(bucket, key, timeout=3600):
    """Wait for file to exist in S3/MinIO"""
    s3 = get_s3_client()
    waiter = s3.get_waiter('object_exists')
    waiter.wait(
        Bucket=bucket,
        Key=key,
        WaiterConfig={'Delay': 30, 'MaxAttempts': timeout // 30}
    )
    print(f"File s3://{bucket}/{key} is now available")


def upload_sample_data(date_str, bucket='fashion-retail-data-lake'):
    """Upload generated sample data for a date"""
    create_bucket_if_not_exists(bucket)

    base_path = os.getenv('DATA_BASE_PATH', '.')
    data_dir = Path(base_path) / 'raw'

    # Upload products
    products_dir = data_dir / 'products' / date_str
    if products_dir.exists():
        for file_path in products_dir.glob('*.csv*'):
            s3_key = f"raw/products/{date_str}/{file_path.name}"
            upload_file_to_s3(str(file_path), bucket, s3_key)

    # Upload transactions
    transactions_dir = data_dir / 'transactions' / date_str
    if transactions_dir.exists():
        for file_path in transactions_dir.glob('*.json*'):
            s3_key = f"raw/transactions/{date_str}/{file_path.name}"
            upload_file_to_s3(str(file_path), bucket, s3_key)

    print(f"Uploaded sample data for {date_str} to {bucket}")


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        date_str = sys.argv[1]
        upload_sample_data(date_str)
    else:
        print("Usage: python s3_minio_tools.py <date_str>")
