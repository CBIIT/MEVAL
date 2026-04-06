import boto3
from botocore.exceptions import ClientError
from urllib.parse import urlparse
import json
import os
from pytz import timezone
from datetime import datetime


def get_time() -> str:
    """Returns the current time"""
    tz = timezone("EST")
    now = datetime.now(tz)
    dt_string = now.strftime("%Y%m%d_T%H%M%S")
    return dt_string


def file_ul(bucket: str, output_folder: str, sub_folder: str, newfile: str):
    """File upload using bucket name, output folder name
    and filename
    """
    # Set the s3 resource object for local or remote execution
    s3 = set_s3_resource()
    source = s3.Bucket(bucket)
    # upload files outside inputs/ folder
    file_key = os.path.join(output_folder, sub_folder, newfile)
    # extra_args={'ACL': 'bucket-owner-full-control'}
    source.upload_file(newfile, file_key)  # , extra_args)
    return None

def set_s3_resource():
    """This method sets the s3_resource object to either use localstack
    for local development if the LOCALSTACK_ENDPOINT_URL variable is
    defined and returns the object
    """
    localstack_endpoint = os.environ.get("LOCALSTACK_ENDPOINT_URL")
    if localstack_endpoint != None:
        AWS_REGION = "us-east-1"
        AWS_PROFILE = "localstack"
        ENDPOINT_URL = localstack_endpoint
        boto3.setup_default_session(profile_name=AWS_PROFILE)
        s3_resource = boto3.resource(
            "s3", region_name=AWS_REGION, endpoint_url=ENDPOINT_URL
        )
    else:
        s3_resource = boto3.resource("s3")
    return s3_resource


def parse_file_url(url: str) -> tuple:
    # in case the url doesn't start with s3://
    if not url.startswith("s3://"):
        url = "s3://" + url
    else:
        pass
    parsed_url = urlparse(url)
    bucket_name = parsed_url.netloc
    object_key = parsed_url.path
    if object_key[0] == "/":
        object_key = object_key[1:]
    else:
        pass
    return bucket_name, object_key


def get_secret(secret_name_path: str, secret_key_name: str):
    """Retrieve a secret hash from AWS Secrets Manager

    Args:
        secret_name_path (str): Secrets name path, i.e. ccdi/storage/inventory/token
        secret_key_name (str): Secret key name associated with hash/token

    Returns:
        str: Secret hash/token
    """
    region_name = "us-east-1"
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name_path)
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    return json.loads(get_secret_value_response["SecretString"])[secret_key_name]


def get_secret_centralized_worker(
    secret_name_path: str, secret_key_name: str, account: str
)-> str:
    """Retrieve a secret hash from AWS Secrets Manager using a centralized worker

    Args:
        secret_name_path (str): Secrets name path, i.e. ccdi/storage/inventory/token
        secret_key_name (str): Secret key name associated with hash/token
        account (str): AWS account identifier
    Returns:
        str: Secret hash/token
    """
    region_name = "us-east-1"
    secret_name_path = (
        f"arn:aws:secretsmanager:{region_name}:{account}:secret:{secret_name_path}"
    )
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name_path)
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    return json.loads(get_secret_value_response["SecretString"])[secret_key_name]


def file_dl_s3(bucket, filename) -> str:
    """File download using bucket name and filename
    filename is the key path in bucket
    file is the basename
    Args:
        bucket (str): S3 bucket name
        filename (str): S3 file key path
    Returns:
        str: downloaded file name
    """
    # Set the s3 resource object for local or remote execution
    s3 = set_s3_resource()
    source = s3.Bucket(bucket)
    file_key = filename
    file = os.path.basename(filename)
    try:
        source.download_file(file_key, file)
        return file
    except ClientError as ex:
        ex_code = ex.response["Error"]["Code"]
        ex_message = ex.response["Error"]["Message"]
        print(
            f"ClientError occurred while downloading file {filename} from bucket {bucket}:\n{ex_code}, {ex_message}"
        )
        raise


def file_ul_s3(bucket: str, output_folder: str, sub_folder: str, newfile: str):
    """File upload to s3 bucket using bucket name, output folder name
    and filename
    Args:
        bucket (str): S3 bucket name
        output_folder (str): S3 output folder name
        sub_folder (str): S3 sub folder name
        newfile (str): local file name to upload
    """
    # Set the s3 resource object for local or remote execution
    s3 = set_s3_resource()
    source = s3.Bucket(bucket)
    # upload files outside inputs/ folder
    file_key = os.path.join(output_folder, sub_folder, newfile)
    # extra_args={'ACL': 'bucket-owner-full-control'}
    source.upload_file(newfile, file_key)  # , extra_args)
    return None


def folder_dl_s3(bucket: str, remote_folder: str) -> None:
    """Downloads a remote direcotry folder from s3
    bucket to local. it generates a folder that follows the
    structure in s3 bucket

    for instance, if the remote_folder is "uniq_id/test_folder",
    the local directory will create path of "uniq_id/test_folder"
    Args:
        bucket (str): S3 bucket name
        remote_folder (str): S3 remote folder name
    """
    s3_resource = set_s3_resource()
    bucket_obj = s3_resource.Bucket(bucket)
    for obj in bucket_obj.objects.filter(Prefix=remote_folder):
        if not os.path.exists(os.path.dirname(obj.key)):
            os.makedirs(os.path.dirname(obj.key))
        try:
            bucket_obj.download_file(obj.key, obj.key)
        except NotADirectoryError as err:
            err_str = repr(err)
            print(
                f"Error downloading folder {remote_folder} from bucket {bucket}: {err_str}"
            )
    return None
