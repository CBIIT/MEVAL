from datetime import datetime
from prefect import flow, task
from prefect.task_runners import ThreadPoolTaskRunner
from src.loader import Loader
from src.parser import ModelParser
from neo4j import GraphDatabase
import boto3
from botocore.exceptions import ClientError
import os
import json
from urllib.parse import urlparse
import pandas as pd
from typing import Literal
from prefect.tasks import task_input_hash

import sys
sys.path.insert(0, os.path.abspath("./libs/prefect-toolkit"))
from workflow.validate_submission import download_model_files


DropDownChoices = Literal["ccdi", "icdc", "cds", "c3dc", "ctdc", "ccdi_dcc"]

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


@task(name="Upload file", task_run_name="upload_file_{newfile}")
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


@task(log_prints=True)
def combine_summaries(upsert_node_summary:dict, upser_rel_summary:dict) -> dict:
    return_dict={}
    # both summaries should have the same keys
    keys = upsert_node_summary.keys()
    for key in keys:
        upsert_key_dict = upsert_node_summary[key]
        rel_key_dict = upser_rel_summary[key]
        key_dict = {}
        for subkey in upsert_key_dict.keys():
            if subkey == "properties_set":
                key_dict["node_properties_set"] = upsert_key_dict[subkey]
                key_dict["rel_properties_set"] = rel_key_dict[subkey]
            else:
                key_dict[subkey] = upsert_key_dict[subkey] + rel_key_dict[subkey]
        return_dict[key] = key_dict
    print(f"combined loading summary for all the submission files:")
    print(json.dumps(return_dict, indent=4))
    return return_dict


def cache_key_ignore_loader_parser(context, parameters):
    # Only hash safe inputs, excluding loader and model_parser
    safe_inputs = {k: v for k, v in parameters.items() if k not in ["loader", "model_parser"]}
    return task_input_hash(context, safe_inputs)



@task(name="Upsert nodes of a file", log_prints=True, cache_key_fn=cache_key_ignore_loader_parser)
def upsert_records_one_file(loader: Loader, file_path: str, id_field: str, subgraph_col: str, chunk_size: int = 3000):
    """Prefect task to upsert data nodes from a submission file

    Args:
        loader (Loader): Loader instance
        file_path (str): submission file path
        id_field (str): id field to use for matching purpose
        subgraph_col (str | None): The column indicating subgraph information. Defaults to None.
        chunk_size (int, optional): Chunk size of each processing. Defaults to 3000.
    """
    file_upsert_summary = loader.upsert_file_records(file_path=file_path, id_field=id_field, subgraph_col=subgraph_col, chunk_size=chunk_size)
    return file_upsert_summary


@flow(
    name="Upsert nodes of file list",
    log_prints=True,
    task_runner=ThreadPoolTaskRunner(max_workers=10),
)
def upsert_records_file_list(loader: Loader, file_list: list[str], id_field: str, subgraph_col: str|None = None, chunk_size: int = 3000):
    """Prefect flow to upsert data nodes from a list of submission files

    Args:
        loader (Loader): Loader instance
        file_list (list[str]): List of submission file paths
        id_field (str): id field to use for matching purpose
        subgraph_col (str | None, optional): The column indicating subgraph information. Defaults to None.
        chunk_size (int, optional): Chunk size of each processing. Defaults to 3000.
    """
    futures = []
    return_dict = {}
    
    for file in file_list:
        future = upsert_records_one_file.submit(
            loader=loader,
            file_path=file,
            id_field=id_field,
            subgraph_col=subgraph_col,
            chunk_size=chunk_size
        )
        futures.append((file, future))
    
    for file, future in futures:
        return_dict[file] = future.result()
    return return_dict


@task(
    name="Upsert relationships of a file", log_prints=True, cache_key_fn=cache_key_ignore_loader_parser
)
def upsert_rels_one_file(
    loader: Loader,
    file_path: str,
    model_parser: ModelParser,
    id_field: str,
    chunk_size: int = 3000,
    delimiter: str = ";",
):
    """Prefect task to upsert data relationships from a submission file

    Args:
        loader (Loader): Loader instance
        file_path (str): submission file path
        model_parser (ModelParser): ModelParser instance
        id_field (str): id field to use for matching purpose
        subgraph_col (str | None): The column indicating subgraph information. Defaults to None.
        chunk_size (int, optional): Chunk size of each processing. Defaults to 3000.
        delimiter (str, optional): Delimiter for multi-valued linkage fields. Defaults to ";"
    """
    file_upsert_summary = loader.upsert_file_relationships(
        file_path=file_path,
        model_parser=model_parser,
        id_field=id_field,
        chunk_size=chunk_size,
        delimiter=delimiter,
    )
    return file_upsert_summary


@flow(
    name="Upsert relationships of file list",
    log_prints=True,
    task_runner=ThreadPoolTaskRunner(max_workers=10),
)
def upsert_rels_file_list(
    loader: Loader,
    file_list: list[str],
    model_parser: ModelParser,
    id_field: str,
    chunk_size: int = 3000,
    delimiter: str = ";",
):
    """Prefect flow to upsert data relationships from a list of submission files

    Args:
        loader (Loader): Loader instance
        file_list (list[str]): List of submission file paths
        model_parser (ModelParser): ModelParser instance
        id_field (str): id field to use for matching purpose
        subgraph_col (str | None, optional): The column indicating subgraph information. Defaults to None.
        chunk_size (int, optional): Chunk size of each processing. Defaults to 3000.
        delimiter (str, optional): Delimiter for multi-valued linkage fields. Defaults to ";"
    """
    futures = []
    return_dict = {}
    
    for file in file_list:
        future = upsert_rels_one_file.submit(
            loader=loader,
            file_path=file,
            model_parser=model_parser,
            id_field=id_field,
            chunk_size=chunk_size,
            delimiter=delimiter
        )
        futures.append((file, future))
    
    for file, future in futures:
        return_dict[file] = future.result()
    return return_dict


@flow(log_prints=True, name="Dataloading Upsert Workflow")
def upsert_files(
    output_bucket_loc: str,
    uri: str,
    tsv_folder_s3uri: str,
    commons_acronym: DropDownChoices,
    tag: str = "",
    id_field: str = "id",
    delimiter: str = ";",
    subgraph_col: str | None = None,
    username: str | None = None,
    password: str | None = None,
):
    """
    Upsert study data from TSV files located in the specified S3 URI into the Neo4j database.

    Args:
        output_bucket_loc (str): The S3 URI of the output location, e.g., s3://my-bucket/runner/output.
        uri (str): The Neo4j database URI.
        tsv_folder_s3uri (str): The S3 URI of the folder containing TSV files, e.g., s3://data-bucket/tsv-folder/.
        commons_acronym (DropDownChoices): The acronym of the data commons model to use. The acceptable values are "ccdi", "icdc", "cds", "c3dc", "ctdc", "ccdi_dcc".
        tag (str, optional): The tag of the data model to use. Defaults to "" to use master branch.
        id_field (str, optional): The field to use as the unique identifier for nodes. Defaults to "id".
        delimiter (str, optional): The delimiter used in multi-valued fields. Defaults to ";"
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

    # test downloading model files
    data_model_yaml, props_yaml = download_model_files(commons_acronym=commons_acronym, tag=tag)
    print(f"Downloaded data model yaml: {data_model_yaml}")
    print(f"Downloaded properties yaml: {props_yaml}")
    # create model parser
    model_parser = ModelParser(
        model_file=data_model_yaml,
        props_file=props_yaml,
        handle=commons_acronym,
    )

    # create index in memgraph instance if not exist
    index_in_db = myloader.create_index(model_parser=model_parser, id_field=id_field)
    print(f"Index created in the database (if not exist): {index_in_db}")

    # download tsv folder
    tsv_bucket, tsv_folder = parse_file_url(tsv_folder_s3uri)
    folder_dl(tsv_bucket, tsv_folder)
    file_list = [
        os.path.join(tsv_folder, f)
        for f in os.listdir(tsv_folder)
        if f.endswith(".tsv")
    ]
    print(f"tsv files to be processed: {*file_list,}")

    # upsert tsv files
    # first to load all the nodes
    # node_upsert_summary = {}
    # for file in file_list:
    #     node_upsert_summary[file] = myloader.upsert_file_records(
    #         file_path=file, id_field=id_field, subgraph_col=subgraph_col, chunk_size=3000
    #     )
    node_upsert_summary = upsert_records_file_list(
        loader=myloader,
        file_list=file_list,
        id_field=id_field,
        subgraph_col=subgraph_col,
        chunk_size=3000,
    )
    print("Print out node upsert summary:")
    print(json.dumps(node_upsert_summary, indent=4))

    # second to load all the relationships
    # rel_upsert_summary = {}
    # for file in file_list:
    #     rel_upsert_summary[file] = myloader.upsert_file_relationships(
    #         file_path=file, model_parser=model_parser, id_field=id_field, chunk_size=3000, delimiter=delimiter
    #     )
    rel_upsert_summary = upsert_rels_file_list(
        loader=myloader,
        file_list=file_list,
        model_parser=model_parser,
        id_field=id_field,
        chunk_size=3000,
        delimiter=delimiter,
    )
    print("Print out relationship upsert summary:")
    print(json.dumps(rel_upsert_summary, indent=4))

    # close myloader instance when the upload is done
    myloader.close()

    # combine two summaries into one, and write into a tsv
    # needs to combine two dict for every file
    combined_summary = combine_summaries(node_upsert_summary, rel_upsert_summary)
    # combined_summary = {k: node_upsert_summary[k] + rel_upsert_summary[k] for k in node_upsert_summary}
    summary_output_name = f"MEVAL_upsert_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.tsv"
    summary_df = pd.DataFrame.from_dict(combined_summary, orient="index")
    summary_df.index.name = "file_name"
    summary_df = summary_df.reset_index()
    summary_df["file_name"] = summary_df["file_name"].apply(lambda x: os.path.basename(x))
    summary_df.to_csv(summary_output_name, sep="\t", index=False)
    # upload the summaru tsv to s3
    output_bucket, output_key_prefix = parse_file_url(output_bucket_loc)
    file_ul(
        bucket=output_bucket,
        output_folder=output_key_prefix,
        sub_folder="MEVAL_upsert_summaries",
        newfile=summary_output_name,
    )
