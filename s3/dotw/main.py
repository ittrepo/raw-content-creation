import boto3
import json
import os
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
prefix = 'dotw/'


def replace_null_strings(obj):
    """Recursively replace 'NULL' strings with None in any JSON structure"""
    if isinstance(obj, dict):
        return {k: replace_null_strings(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [replace_null_strings(elem) for elem in obj]
    elif obj == "NULL":
        return None
    else:
        return obj


# Step 1: List all JSON files in the folder
response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.json')]

# Step 2: Process each file
for file_key in files:
    print(f"Processing: {file_key}")
    
    try:
        # Download JSON file
        obj = s3.get_object(Bucket=bucket_name, Key=file_key)
        content = obj['Body'].read().decode('utf-8')
        data = json.loads(content)

        # Replace "NULL" with None
        updated_data = replace_null_strings(data)

        # Upload the modified file
        s3.put_object(Bucket=bucket_name, Key=file_key, Body=json.dumps(updated_data, indent=2))
        print(f"Updated: {file_key}")

    except Exception as e:
        print(f"Error processing {file_key}: {e}")
