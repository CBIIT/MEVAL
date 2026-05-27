from datetime import datetime
from prefect import flow, task, get_run_logger
import time
import json
from contextlib import contextmanager
from src.loader import Loader
from neo4j import GraphDatabase
from src.utils import parse_file_url, get_time, file_ul_s3, get_secret_centralized_worker


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


def is_json_empty(filepath: str) -> bool:
    with open(filepath, "r") as f:
        data = json.load(f)
    if len(data) == 0:
        return True
    return False

def find_json_length(filepath: str) -> int:
    with open(filepath, "r") as f:
        data = json.load(f)
    return len(data)

def extract_internal_id_from_json(json_filepath: str, internal_id_key: str) -> str:
    with open(json_filepath, "r") as f:
        data = json.load(f)
    internal_ids = []
    for item in data:
        if internal_id_key in item:
            internal_ids.append(item[internal_id_key])
        else:
            pass
    return internal_ids


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


@flow(log_prints=True, name="Find floating nodes in the database prefect flow")
def find_floating_db_nodes_flow(loader: Loader, output_filename: str, root_node_label: str = "study") -> None:
    floating_nodes_generator = loader.find_nodes_without_path_to_root(
        root_node_label=root_node_label
    )
    write_json_streaming(floating_nodes_generator, output_filename)
    return None

@flow(log_prints=True, name="delete nodes in the database by internal db identifier")
def delete_nodes_by_internal_id_flow(loader: Loader, internal_ids_to_delete: list[int]) -> int:
    deleted_count = loader.delete_nodes_by_internal_id(
        identifier_list=internal_ids_to_delete
    )
    return deleted_count

@flow(log_prints=True, name="Find floating nodes in the database and delete if needed")
def find_floating_db_nodes(
    db_account_id: str,
    db_creds_secret_name: str,
    uri_secret_key: str,
    output_bucket_loc: str,
    root_node_label: str = "study",
    delete_floating_nodes_if_found: bool = False,
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
        delete_floating_nodes_if_found (bool, optional): Whether to delete the floating nodes found in the database. Defaults to False.
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

    output_filename = f"nodes_no_path_to_{root_node_label}_{get_time()}.json"
    find_floating_db_nodes_flow(loader=loader, output_filename=output_filename, root_node_label=root_node_label)
    if is_json_empty(output_filename):
        logger.info("No floating nodes found in the database. Nothing to upload to s3.")
        logger.info("Finished finding floating nodes in the database.")
    else:
        file_length = find_json_length(output_filename)
        logger.warning(f"Found {file_length} floating nodes in the database. Writing to {output_filename}...")
        file_ul_s3(
            bucket=output_bucket,
            output_folder=output_folder,
            sub_folder="",
            newfile=output_filename,
        )
        logger.warning(f"Uploaded to s3://{output_bucket}/{output_folder}/{output_filename}")
        logger.info("Finished finding floating nodes in the database.")

        # if runner decides to delete the floating nodes
        if delete_floating_nodes_if_found:
            logger.warning("delete_floating_nodes_if_found flag is set to True. Deleting floating nodes from the database...")
            internal_id_key = "db_internal_id"
            internal_ids_to_delete = extract_internal_id_from_json(output_filename, internal_id_key)
            deleted_count = delete_nodes_by_internal_id_flow(loader=loader, internal_ids_to_delete=internal_ids_to_delete)
            logger.info(f"Deleted {deleted_count} floating nodes from the database.")
        else:
            logger.info("delete_floating_nodes_if_found flag is set to False. Floating nodes will not be deleted from the database.")
        logger.info("Workflow finished.")
    loader.close()
    return None
