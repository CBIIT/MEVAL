from prefect import flow, task
from src.loader import Loader
from src.parser import ModelParser
from neo4j import GraphDatabase
import boto3
from botocore.exceptions import ClientError
import os
import json
from urllib.parse import urlparse


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


@task(name="Download file", task_run_name="download_file_{filename}", log_prints=True)
def file_dl(bucket, filename) -> str:
    """File download using bucket name and filename
    filename is the key path in bucket
    file is the basename
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


@task(
    name="Download folder",
    task_run_name="download_folder_{remote_folder}",
    log_prints=True,
)
def folder_dl(bucket: str, remote_folder: str) -> None:
    """Downloads a remote direcotry folder from s3
    bucket to local. it generates a folder that follows the
    structure in s3 bucket

    for instance, if the remote_folder is "uniq_id/test_folder",
    the local directory will create path of "uniq_id/test_folder"
    """
    s3_resouce = set_s3_resource()
    bucket_obj = s3_resouce.Bucket(bucket)
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


@flow(log_prints=True)
def upsert_files(
    uri: str,
    tsv_folder_s3uri: str,
    id_field: str,
    model_file_s3uri: str,
    props_file_s3uri: str,
    subgraph_col: str = None,
    username: str = None,
    password: str = None,
):
    """
    Upsert study data from TSV files located in the specified S3 URI into the Neo4j database.

    Args:
        uri (str): The Neo4j database URI.
        tsv_folder_s3uri (str): The S3 URI of the folder containing TSV files.
        id_field (str): The field to use as the unique identifier for nodes.
        model_file_s3uri (str): The S3 URI of the model yaml file.
        props_file_s3uri (str): The S3 URI of the properties yaml file.
        subgraph_col (str, optional): The column indicating subgraph information. Defaults to None.
        username (str, optional): Username for Neo4j authentication. Defaults to None.
        password (str, optional): Password for Neo4j authentication. Defaults to None.
    """
    # create a loader instance
    if username is not None and password is not None:
        driver = GraphDatabase.driver(uri, auth=(username, password))
    else:
        driver = GraphDatabase.driver(uri)
    myloader = Loader(driver=driver)

    # download model file and props file
    model_file_bucket, model_file_key = parse_file_url(model_file_s3uri)
    props_file_bucket, props_file_key = parse_file_url(props_file_s3uri)

    # download the files
    model_file_name = file_dl(model_file_bucket, model_file_key)
    props_file_name = file_dl(props_file_bucket, props_file_key)

    # create model parser
    model_parser = ModelParser(
        model_file=model_file_name,
        props_file=props_file_name,
        handle="handle",
    )

    # download tsv folder
    tsv_bucket, tsv_folder = parse_file_url(tsv_folder_s3uri)
    folder_dl(tsv_bucket, tsv_folder)
    file_list = [
        os.path.join(tsv_folder, f)
        for f in os.listdir(tsv_folder)
        if f.endswith(".tsv")
    ]

    # upsert tsv files
    # first to load all the nodes
    node_upsert_summary = {}
    for file in file_list:
        node_upsert_summary[file] = myloader.upsert_file_records(
            file_path=file, id_field=id_field, subgraph_col=subgraph_col
        )
    print(json.dumps(node_upsert_summary, indent=4))

    # second to load all the relationships
    rel_upsert_summary = {}
    for file in file_list:
        rel_upsert_summary[file] = myloader.upsert_file_relationships(
            file_path=file, model_parser=model_parser, id_field=id_field
        )
    print(json.dumps(rel_upsert_summary, indent=4))
