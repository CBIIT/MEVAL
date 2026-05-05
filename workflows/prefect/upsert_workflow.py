from datetime import datetime
from prefect import flow, task
from prefect.cache_policies import NO_CACHE
from src.loader import Loader
from src.validator import Validator
from src.parser import ModelParser
from src.utils import (
    parse_file_url,
    get_secret_centralized_worker,
    file_dl_s3,
    file_ul_s3,
    folder_dl_s3,
    get_time,
)
from neo4j import GraphDatabase
import os
import pandas as pd
from typing import Literal
import logging
from timeit import default_timer as timer
import sys
import json

sys.path.insert(0, os.path.abspath("./libs/prefect-toolkit"))
from workflow.validate_submission import download_model_files

DropDownChoices = Literal["ccdi", "icdc", "cds", "c3dc", "ctdc", "ccdi_dcc", "popsci"]


@task(name="Get secret from AWS secrets manager")
def get_secret_task(account: str, secret_name_path: str, secret_key_name: str) -> str:
    """Prefect task to retrieve a secret hash from AWS Secrets Manager

    Args:
        account (str): AWS account identifier
        secret_name_path (str): Secrets name path, i.e. ccdi/storage/inventory/token
        secret_key_name (str): Secret key name associated with hash/token

    Returns:
        str: Secret hash/token
    """
    secret_value = get_secret_centralized_worker(
        secret_name_path=secret_name_path,
        secret_key_name=secret_key_name,
        account=account,
    )
    return secret_value


@task(
    name="Download file from s3",
    task_run_name="download_file_{filename}",
    log_prints=True,
)
def file_dl(bucket, filename) -> str:
    """Prefect task to download a file from s3 using bucket name and filename
    filename is the key path in bucket
    file is the basename
    Args:
        bucket (str): S3 bucket name
        filename (str): S3 file key path
    Returns:
        str: downloaded file name
    """
    filename = file_dl_s3(bucket=bucket, filename=filename)
    return filename


@task(name="Upload file to s3", task_run_name="upload_file_{newfile}")
def file_ul(bucket: str, output_folder: str, sub_folder: str, newfile: str):
    """Prefect task to upload file to s3 bucket using bucket name, output folder name
    and filename
    """
    file_ul_s3(
        bucket=bucket,
        output_folder=output_folder,
        sub_folder=sub_folder,
        newfile=newfile,
    )
    return None


@task(
    name="Download folder",
    task_run_name="download_folder_{remote_folder}",
    log_prints=True,
)
def folder_dl(bucket: str, remote_folder: str) -> None:
    """Prefect task to download a remote direcotry folder from s3
    bucket to local. it generates a folder that follows the
    structure in s3 bucket

    for instance, if the remote_folder is "uniq_id/test_folder",
    the local directory will create path of "uniq_id/test_folder"
    """
    folder_dl_s3(bucket=bucket, remote_folder=remote_folder)
    return None


@task(
    name="Upsert nodes of file list",
    log_prints=True,
    cache_policy=NO_CACHE,
)
def upsert_records_file_list(
    loader: Loader,
    file_list: list[str],
    model_parser: ModelParser,
    id_field: str,
    subgraph_col: str | None = None,
    chunk_size: int = 3000,
    delimiter: str = ";",
    logger: logging.Logger | None = None,
):
    """Prefect flow to upsert data nodes from a list of submission files

    Args:
        loader (Loader): Loader instance
        file_list (list[str]): List of submission file paths
        model_parser (ModelParser): ModelParser instance
        id_field (str): id field to use for matching purpose
        subgraph_col (str | None, optional): The column indicating subgraph information. Defaults to None.
        chunk_size (int, optional): Chunk size of each processing. Defaults to 3000.
    """
    return_dict = {}
    for file in file_list:
        proc_begin = timer()
        record_upsert_summary = loader.upsert_file_records(
            file_path=file,
            model_parser=model_parser,
            id_field=id_field,
            subgraph_col=subgraph_col,
            chunk_size=chunk_size,
            delimiter=delimiter,
            logger=logger,
        )
        proc_end = timer()
        if logger:
            logger.info(f"Time consumed (sec): {proc_end - proc_begin:.2f}")
        print(f"Time consumed (sec): {proc_end - proc_begin:.2f}")
        return_dict[file] = record_upsert_summary
    return return_dict


@task(
    name="Upsert relationships of file list",
    log_prints=True,
    cache_policy=NO_CACHE,
)
def upsert_rels_file_list(
    loader: Loader,
    file_list: list[str],
    model_parser: ModelParser,
    id_field: str,
    chunk_size: int = 3000,
    delimiter: str = ";",
    logger: logging.Logger | None = None,
):
    """Prefect task to upsert data relationships from a list of submission files

    Args:
        loader (Loader): Loader instance
        file_list (list[str]): List of submission file paths
        model_parser (ModelParser): ModelParser instance
        id_field (str): id field to use for matching purpose
        chunk_size (int, optional): Chunk size of each processing. Defaults to 3000.
        delimiter (str, optional): Delimiter for multi-valued linkage fields. Defaults to ";"
    """
    return_dict = {}
    processed_rel_dict = {}
    for file in file_list:
        proc_begin = timer()
        rel_upsert_summary, processed_rel_dict = loader.upsert_file_relationships(
            file_path=file,
            model_parser=model_parser,
            processed_rel_dict=processed_rel_dict,
            id_field=id_field,
            chunk_size=chunk_size,
            delimiter=delimiter,
            logger=logger,
        )
        proc_end = timer()
        if logger:
            logger.info(f"Time consumed (sec): {proc_end - proc_begin:.2f}")
        print(f"Time consumed (sec): {proc_end - proc_begin:.2f}")
        return_dict[file] = rel_upsert_summary
    return return_dict, processed_rel_dict


@task(name="Combine node and relationship upsert summaries", log_prints=True)
def combine_summaries(upsert_node_summary: dict, upsert_rel_summary: dict) -> dict:
    """Combines node upsert summary dict with relationship upsert summary dict

    Args:
        upsert_node_summary (dict): summary dictionary from node upsert
        upsert_rel_summary (dict): summary dictionary from relationship upsert

    Returns:
        dict: a combined summary dictionary

    """
    return_dict = {}
    keys = upsert_node_summary.keys()
    for key in keys:
        upsert_key_dict = upsert_node_summary[key]
        rel_key_dict = upsert_rel_summary.get(key, {})  # graceful missing key
        key_dict = {}
        for subkey in upsert_key_dict.keys():
            if subkey == "properties_set":
                key_dict["node_properties_set"] = upsert_key_dict[subkey]
                key_dict["rel_properties_set"] = rel_key_dict.get(
                    subkey, 0
                )  # in case of missing key
            else:
                # use .get() with 0 default for keys that don't exist in rel_key_dict
                # e.g. labels_added, nodes_created are node-only keys
                key_dict[subkey] = upsert_key_dict[subkey] + rel_key_dict.get(subkey, 0)
        # also capture rel-only keys like relationships_created
        for subkey in rel_key_dict.keys():
            if subkey not in upsert_key_dict and subkey != "properties_set":
                key_dict[subkey] = rel_key_dict[subkey]
        return_dict[key] = key_dict
    return return_dict


@task(name="Prepare upsert summary into tsv", log_prints=True)
def prepare_upsert_summary_tsv(combined_summary: dict) -> str:
    """Prepares upsert summary dictionary into a tsv file

    Args:
        combined_summary (dict): a combined summary dictionary including node and relationship upsert summaries

    Returns:
        str: file name of a summary tsv file
    """
    summary_output_name = (
        f"MEVAL_upsert_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.tsv"
    )
    summary_df = pd.DataFrame.from_dict(combined_summary, orient="index")
    summary_df.index.name = "file_name"
    summary_df = summary_df.reset_index()
    summary_df["file_name"] = summary_df["file_name"].apply(
        lambda x: os.path.basename(x)
    )
    summary_df.to_csv(summary_output_name, sep="\t", index=False)
    return summary_output_name


def get_logger(log_file: str) -> logging.Logger:
    """Returns a logger instance that records loading progress

    Args:
        log_file (str): file path including the filename

    Returns:
        logging.Logger: Logger instance
    """
    logger = logging.getLogger("upsert_logger")
    logger.setLevel(logging.INFO)

    # Avoid adding duplicate handlers if called multiple times
    if not logger.handlers:
        file_handler = logging.FileHandler(log_file)
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    return logger


@flow(
    log_prints=True,
    name="Dataloading Upsert Workflow",
    flow_run_name="loading_{commons_acronym}_{tag}_" + f"{get_time()}",
)
def upsert_files(
    db_account_id: str,
    db_creds_secret_name: str,
    uri_secret_key: str,
    output_bucket_loc: str,
    tsv_folder_s3uri: str,
    commons_acronym: DropDownChoices,
    tag: str = "",
    uuid_field: str = "guid",
    delimiter: str = ";",
    subgraph_col: str | None = None,
    username_secret_key: str | None = None,
    password_secret_key: str | None = None,
):
    """
    Upsert data from TSV files into a graph database.

    Args:
        db_creds_secret_name (str): The name/path of the AWS Secrets Manager secret containing the database credentials.
        uri_secret_key (str): The secret key name for the database URI within the secret.
        output_bucket_loc (str): The S3 URI of the output location, e.g., s3://my-bucket/runner/output.
        tsv_folder_s3uri (str): The S3 URI of the folder containing TSV files, e.g., s3://data-bucket/tsv-folder/.
        commons_acronym (DropDownChoices): The acronym of the data commons model to use. The acceptable values are "ccdi", "icdc", "cds", "c3dc", "ctdc", "ccdi_dcc".
        tag (str, optional): The tag of the data model to use. Defaults to "" to use master branch.
        uuid_field (str, optional): The field to use as the unique identifier for each data entry. Defaults to "guid".
        delimiter (str, optional): The delimiter used in multi-valued fields. Defaults to ";"
        subgraph_col (str, optional): The column indicating subgraph information. Defaults to None.
        username_secret_key (str, optional): The secret key name for the username to access the DB instance within the secret. Defaults to None.
        password_secret_key (str, optional): The secret key name for the password to access the DB instance within the secret. Defaults to None.
    """
    # retrieve db creds from AWS secrets manager
    uri = get_secret_task(
        account=db_account_id,
        secret_name_path=db_creds_secret_name,
        secret_key_name=uri_secret_key,
    )
    if username_secret_key is not None and password_secret_key is not None:
        username = get_secret_task(
            account=db_account_id,
            secret_name_path=db_creds_secret_name,
            secret_key_name=username_secret_key,
        )
        password = get_secret_task(
            account=db_account_id,
            secret_name_path=db_creds_secret_name,
            secret_key_name=password_secret_key,
        )
        driver = GraphDatabase.driver(uri, auth=(username, password))
    else:
        driver = GraphDatabase.driver(uri)

    myloader = Loader(driver=driver)

    # create a logger instance to record logger info in a file
    file_logger_name = f"upsert_workflow_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    file_logger = get_logger(log_file=file_logger_name)

    # download model files
    data_model_yaml, props_yaml = download_model_files(
        commons_acronym=commons_acronym, tag=tag
    )
    file_logger.info(
        f"Downloaded data model, props yaml: {data_model_yaml}, {props_yaml}"
    )
    print(f"Downloaded data model yaml: {data_model_yaml}")
    print(f"Downloaded properties yaml: {props_yaml}")
    # create model parser
    model_parser = ModelParser(
        model_file=data_model_yaml,
        props_file=props_yaml,
        handle=commons_acronym,
    )

    # create index in memgraph instance if not exist
    index_in_db = myloader.create_index(model_parser=model_parser, id_field=uuid_field)
    index_df = pd.DataFrame(index_in_db)
    print(
        f"Index created in the database (if not exist):\n\t{index_df.to_markdown(tablefmt='rounded_grid', index=False).replace('\n', '\n\t')}"
    )
    file_logger.info(
        f"Index created in the database (if not exist):\n\t{index_df.to_markdown(tablefmt='rounded_grid', index=False).replace('\n', '\n\t')}"
    )

    # download tsv folder
    tsv_bucket, tsv_folder = parse_file_url(tsv_folder_s3uri)
    folder_dl(tsv_bucket, tsv_folder)
    # search for tsv files recursively under tsv_folder
    file_list = Validator.find_tsv_files(tsv_folder)
    file_list_names = [os.path.basename(f) for f in file_list]
    print(f"File list to be processed: {*file_list_names,}")
    file_logger.info(f"File counts to be processed: {len(file_list)}")

    # upsert tsv files
    # first to load all the nodes
    print("Starting node upsert...")
    file_logger.info("(Node Upsert) Starting node upsert...")
    node_upsert_summary = upsert_records_file_list(
        loader=myloader,
        model_parser=model_parser,
        file_list=file_list,
        id_field=uuid_field,
        subgraph_col=subgraph_col,
        chunk_size=3000,
        delimiter=delimiter,
        logger=file_logger,
    )
    total_nodes_created = sum(
        [value["nodes_created"] for value in node_upsert_summary.values()]
    )
    total_node_prop_set = sum(
        [value["properties_set"] for value in node_upsert_summary.values()]
    )
    print("Node Upsert is complete.")
    print(f"Nodes created: {total_nodes_created}")
    print(f"Node properties set: {total_node_prop_set}")
    file_logger.info("Node Upsert is complete.")
    file_logger.info(f"Nodes created: {total_nodes_created}")
    file_logger.info(f"Node properties set: {total_node_prop_set}")

    # second to load all the relationships
    print("Starting relationship upsert...")
    file_logger.info("Starting relationship upsert...")
    rel_upsert_summary, _ = upsert_rels_file_list(
        loader=myloader,
        file_list=file_list,
        model_parser=model_parser,
        id_field=uuid_field,
        chunk_size=3000,
        delimiter=delimiter,
        logger=file_logger,
    )
    # get total relationships created and props set
    total_rels_created = sum(
        [value["relationships_created"] for value in rel_upsert_summary.values()]
    )
    total_rel_prop_set = sum(
        [value["properties_set"] for value in rel_upsert_summary.values()]
    )
    print("Relationship Upsert is complete.")
    print(f"Relationships created: {total_rels_created}")
    print(f"Relationship properties set: {total_rel_prop_set}")
    file_logger.info("Relationship Upsert is complete.")
    file_logger.info(f"Relationships created: {total_rels_created}")
    file_logger.info(f"Relationship properties set: {total_rel_prop_set}")

    # combine two summaries into one, and write into a tsv
    # needs to combine two dict for every file
    combined_summary = combine_summaries(node_upsert_summary, rel_upsert_summary)
    tsv_output = prepare_upsert_summary_tsv(combined_summary=combined_summary)
    # upload the summaru tsv to s3
    output_bucket, output_key_prefix = parse_file_url(output_bucket_loc)
    file_ul(
        bucket=output_bucket,
        output_folder=output_key_prefix,
        sub_folder="MEVAL_upsert_summaries",
        newfile=tsv_output,
    )
    # upload the log file to s3
    file_ul(
        bucket=output_bucket,
        output_folder=output_key_prefix,
        sub_folder="MEVAL_upsert_summaries",
        newfile=file_logger_name,
    )
    # close myloader instance when the upload is done
    myloader.close()
