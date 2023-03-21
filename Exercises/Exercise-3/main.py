import boto3

# Import required packages and establish session with default credentials

import boto3
import os
import io
import gzip
import shutil

# Replace 'bucket-name' and 'object-key' with the actual values
bucket_name = "commoncrawl"
object_key = "crawl-data/CC-MAIN-2022-05/wet.paths.gz"
project_path = "/Users/blakeenyart/programming/projects/sandbox/data-engineering-practice/Exercises/Exercise-3"

def setup_session() -> boto3.Session:
    """ Set up Boto3 session
    """
    session = boto3.Session()

    # Check if an active token is present
    sts_client = session.client("sts")
    try:
        sts_client.get_caller_identity()
        print("Active token found!")
    except boto3.exceptions.botocore.exceptions.NoCredentialsError:
        print("No active token found.")
        # Prompt for MFA code
        mfa_token = input("Enter MFA token code: ")

        # Assume role with MFA credentials
        sts_client = session.client("sts")
        response = sts_client.assume_role(
            RoleArn="arn:aws:iam::251357961920:role/AdminAccess",
            RoleSessionName="my-role-session",
            TokenCode=mfa_token,
        )

        # Set up a new Boto3 session with the assumed role credentials
        session = boto3.Session(
            aws_access_key_id=response["Credentials"]["AccessKeyId"],
            aws_secret_access_key=response["Credentials"]["SecretAccessKey"],
            aws_session_token=response["Credentials"]["SessionToken"],
        )
        print("Assumed role with MFA credentials!")
    return session
    

def extract_url(s3) -> str:
    # Get the object from the bucket
    content = s3.get_object(Bucket=bucket_name, Key=object_key)['Body'].read()
    extracted_file_path = os.path.join(project_path, object_key.split('/')[-1][:-3])

    next_object_key = ''

    # Extract the .gz file to a new file
    with gzip.GzipFile(fileobj=io.BytesIO(content)) as file:
        for line in file:
            clean_line = line.decode().strip()
            next_object_key = clean_line
            break

    return next_object_key

def print_object_info(s3, next_object_key: str) -> None:
    # Download much larger file indicated from the file path above
    next_local_gz_path = os.path.join(project_path, next_object_key.split('/')[-1])

    # Get the object from the bucket
    print("Downloading large file...")
    s3.download_file(bucket_name, next_object_key, next_local_gz_path)
    print("File downloaded.")
    # Extract the .gz file to a new file
    next_extracted_file_path = os.path.join(project_path, next_object_key.split('/')[-1][:-3])

    with gzip.open(next_local_gz_path, "rb") as f_in:
        with open(next_extracted_file_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
    # Stream the output of the file to the terminal
    with open(next_extracted_file_path) as file:
        i = 0
        for line in file:
            assert line # Ensure there is data
            print(line.strip())
            # Limit output to reduce local usage
            i += 1
            if i == 10:
                break

    # File clean-up afterwards
    os.remove(next_extracted_file_path)
    os.remove(next_local_gz_path)


def main():
    session = setup_session()

    # Open S3 connection and set variables
    s3 = session.client("s3")
    next_object_key = extract_url(s3)
    print_object_info(s3, next_object_key)

if __name__ == '__main__':
    main()
