from datetime import datetime
from prefect import flow, get_run_logger
import time
import json
from contextlib import contextmanager
from src.loader import Loader
from neo4j import GraphDatabase
from workflows.prefect.upsert_workflow import get_secret_task
from src.utils import parse_file_url, get_time, file_ul_s3


def write_json_streaming(generator, filepath):
    with open(filepath, 'w') as f:
        f.write('[')
        first = True
        for item in generator:
            if not first:
                f.write(',')
            json.dump(item, f)
            first = False
        f.write(']')


@flow(log_prints=True, name="Find floating nodes in the database")
def find_floating_db_nodes(
    db_account_id: str,
    db_creds_secret_name: str,
    uri_secret_key: str,
    output_bucket_loc: str,
    root_node_label: str = "study",
    username_secret_key: str | None = None,
    password_secret_key: str | None = None,
):
    """
    Find any node that are not in a path to the root node (e.g. study or program) in the graph database, which means these  nodes don't belong to any study or program subgraph and are considered as floating nodes.
    This workflow will return a list of floating nodes with their internal db identifier, type and properties.

    Args:
        db_account_id (str): AWS account identifier for retrieving secrets.
        db_creds_secret_name (str): The name of the AWS Secrets Manager secret containing database credentials.
        uri_secret_key (str): The key for the database URI in the secret.
        output_bucket_loc (str): The S3 bucket location to store the output list of floating nodes.
        root_node_label (str, optional): The label of the root node, such as "study" or "program". Defaults to "study".
        username_secret_key (str | None, optional): The key for the database username in the secret. Defaults to None.
        password_secret_key (str | None, optional): The key for the database password in the secret. Defaults to None.
    """
    logger = get_run_logger()
    logger.info("Starting to retrieve database credentials from Secrets Manager...")
    uri = get_secret_task(
        account=db_account_id,
        secret_name_path=db_creds_secret_name,
        secret_key_name=uri_secret_key,
    )
    username = (
        get_secret_task(
            account=db_account_id,
            secret_name_path=db_creds_secret_name,
            secret_key_name=username_secret_key,
        )
        if username_secret_key
        else None
    )
    password = (
        get_secret_task(
            account=db_account_id,
            secret_name_path=db_creds_secret_name,
            secret_key_name=password_secret_key,
        )
        if password_secret_key
        else None
    )

    driver = GraphDatabase.driver(
        uri, auth=(username, password) if username and password else None
    )

    output_bucket, output_folder = parse_file_url(output_bucket_loc)
    loader = Loader(driver=driver)
    logger.info("Finding floating nodes in the database...")
    floating_nodes_generator = loader.find_nodes_without_path_to_root(
        root_node_label=root_node_label
    )

    output_filename = f"node_no_path_to_{root_node_label}_{get_time()}.json"
    write_json_streaming(floating_nodes_generator, output_filename)
    logger.info(f"Finished writing floating nodes to {output_filename}. Uploading to S3...")
    file_ul_s3(
        bucket=output_bucket,
        output_folder=output_folder,
        sub_folder=None,
        newfile=output_filename,
    )
    logger.info(f"Finished uploading {output_filename} to s3://{output_bucket}/{output_folder}/")
    return None
