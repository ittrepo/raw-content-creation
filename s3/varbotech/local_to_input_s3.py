import boto3
import os
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# AWS credentials and setup
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_ACCESS_KEY = os.getenv("S3_SECRET_ACCESS_KEY")
REGION_NAME = os.getenv("REGION_NAME")

s3 = boto3.client(
    's3',
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_ACCESS_KEY,
    region_name=REGION_NAME
)

bucket_name = 'hotels-content-data'
s3_prefix = 'vervotech/'
local_folder = r'D:/content_for_hotel_json/Create_Json_with_supplier/new/vervotech_new_2'

def file_exists_in_s3(bucket, key):
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == "404":
            return False
        else:
            print(f'⚠️ Error checking {key}: {e}')
            return True  # Assume file exists to avoid re-uploading in case of unexpected errors

def upload_file_to_s3(local_path, bucket, key):
    try:
        s3.upload_file(local_path, bucket, key)
        print(f'✅ Uploaded: {key}')
    except Exception as upload_error:
        print(f'❌ Failed to upload {key}: {upload_error}')

# Upload files only if they don't already exist in S3
for file_name in os.listdir(local_folder):
    if file_name.endswith('.json'):
        local_file_path = os.path.join(local_folder, file_name)
        s3_key = s3_prefix + file_name

        if file_exists_in_s3(bucket_name, s3_key):
            print(f'⏩ Skipping (already exists): {file_name}')
        else:
            print(f'⬆️ Uploading: {file_name}')
            upload_file_to_s3(local_file_path, bucket_name, s3_key)
