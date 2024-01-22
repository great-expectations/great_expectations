# This file is used to store information related to where the oss_docs_versions zip file is stored,
# which contains the content used to build earlier versions of the docs.

S3_BUCKET = "superconductive-public"
REGION = "us-east-2"
FILENAME = "oss_docs_versions_20240118.zip"

S3_URL = f"https://{S3_BUCKET}.s3.{REGION}.amazonaws.com/{FILENAME}"

# Run this file to print the S3_URL
if __name__ == "__main__":
    print(S3_URL)
