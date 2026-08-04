from pathlib import Path

import boto3
from botocore.exceptions import ClientError


BUCKET_NAME = "credit-risk-data"

LOCAL_FILE = Path(
    r"\File\dataset.csv"
)

S3_KEY = "raw/credit_risk_dataset.csv"


def main() -> None:
    if not LOCAL_FILE.exists():
        raise FileNotFoundError(f"CSV file not found: {LOCAL_FILE}")

    s3 = boto3.client(
        "s3",
        endpoint_url="http://localhost:4566",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )

    try:
        s3.head_bucket(Bucket=BUCKET_NAME)
        print(f"Bucket already exists: {BUCKET_NAME}")
    except ClientError:
        s3.create_bucket(Bucket=BUCKET_NAME)
        print(f"Created bucket: {BUCKET_NAME}")

    s3.upload_file(
        str(LOCAL_FILE),
        BUCKET_NAME,
        S3_KEY,
    )

    print(
        f"Uploaded {LOCAL_FILE.name} to "
        f"s3://{BUCKET_NAME}/{S3_KEY}"
    )


if __name__ == "__main__":
    main()