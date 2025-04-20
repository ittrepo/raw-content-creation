import boto3
import os
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()


S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_ACCESS_KEY = os.getenv("S3_SECRET_ACCESS_KEY")
REGION_NAME = os.getenv("REGION_NAME")

# AWS credentials and setup
s3 = boto3.client(
    's3',
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_ACCESS_KEY,
    region_name=REGION_NAME
)

bucket_name = 'hotels-content-data'
s3_prefix = 'vervotech/'

local_folder = r'D:/content_for_hotel_json/Create_Json_with_supplier/new/vervotech_new_2'

# Upload files only if they don't already exist in S3
for file_name in os.listdir(local_folder):
    if file_name.endswith('.json'):
        local_file_path = os.path.join(local_folder, file_name)
        s3_key = s3_prefix + file_name

        # Check if file already exists
        try:
            s3.head_object(Bucket=bucket_name, Key=s3_key)
            print(f'⏩ Skipping (already exists): {file_name}')
        except ClientError as e:
            if e.response['Error']['Code'] == "404":
                # File doesn't exist, so upload it
                try:
                    print(f'⬆️ Uploading: {file_name}')
                    s3.upload_file(local_file_path, bucket_name, s3_key)
                    print(f'✅ Uploaded: {file_name}')
                except Exception as upload_error:
                    print(f'❌ Failed to upload {file_name}: {upload_error}')
            else:
                print(f'⚠️ Error checking {file_name}: {e}')
