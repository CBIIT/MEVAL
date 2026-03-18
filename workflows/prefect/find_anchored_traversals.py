from datetime import datetime
from prefect import flow
import time
import json
from contextlib import contextmanager
from src.loader import Loader
from src.utils import get_time, file_ul
import os
from neo4j import GraphDatabase
from workflows.prefect.upsert_workflow import get_secret_task


@flow(log_prints=True, name="Find Anchored Traversals Flow")
def find_anchored_traversals(
    db_creds_secret_name: str,
    uri_secret_key: str,
    output_bucket: str,
    runner: str,
    root_node_label: str,
    root_node_property: str,
    root_node_property_value: str,
    intermediate_root_node_label: str,
    intermediate_root_node_property: str,
    intermediate_root_node_property_value_list: list[str],
    username_secret_key: str | None = None,
    password_secret_key: str | None = None,
) -> tuple[str, list[int]]:
    """
    Find all descendant traversals from an intermediate root node within a larger rooted subgraph. A common use case is to identify all descendants/leaf nodes from a participant node (intermediate root node) within a study subgraph (root node).

    Args:
        db_creds_secret_name (str): The name of the AWS Secrets Manager secret containing database credentials.
        uri_secret_key (str): The key for the database URI in the secret.
        output_bucket (str): The name of the S3 bucket to store the output files.
        runner (str): A unique runner name which helps to determins the output file path, such as `test_runner_001`. Outputs from this flow run will be uploaded under `s3://{output_bucket}/{runner}/`.
        root_node_label (str): The label of the root node. It is the origin from which all other nodes in its tree structure descend. Root node has no other parent node it points to. In most cases, this root node label is 'study'
        root_node_property (str): The property name of the root node to match, such as 'study_id'
        root_node_property_value (str): The property value of the root node to match.
        intermediate_root_node_label (str): The label of the intermediate root node. This node sits between the root node and its descendants. A common use case is when the intermediate root node is a 'participant' node within a 'study' subgraph.
        intermediate_root_node_property (str): The property name of the intermediate root node to match, such as 'participant_id'
        intermediate_root_node_property_value_list (list[str]): The list of property values of the intermediate root node to match.
        username_secret_key (str | None): The key for the database username in the secret.
        password_secret_key (str | None): The key for the database password in the secret.

    Returns:
        tuple[str, list[int]]: 
    """
    print("Starting to retrieve database credentials from Secrets Manager...")
    uri = get_secret_task(db_creds_secret_name, uri_secret_key)
    username = (
        get_secret_task(db_creds_secret_name, username_secret_key)
        if username_secret_key
        else None
    )
    password = (
        get_secret_task(db_creds_secret_name, password_secret_key)
        if password_secret_key
        else None
    )

    driver = GraphDatabase.driver(
        uri, auth=(username, password) if username and password else None
    )
    myloader = Loader(driver=driver)

    upload_folder_path = f"{runner}/anchored_traversals_{get_time()}"

    for intermediate_root_node_property_value in intermediate_root_node_property_value_list:
        print("Processing intermediate root node property value:", intermediate_root_node_property_value)
        print("Generating anchored traversal visualization...")
        viz_file = myloader.viz_intermediate_anchored_traversals(
            root_node_label=root_node_label,
            root_node_prop=root_node_property,
            root_node_prop_value=root_node_property_value,
            intermediate_root_node_label=intermediate_root_node_label,
            intermediate_root_node_prop=intermediate_root_node_property,
            intermediate_root_node_prop_value=intermediate_root_node_property_value,
            viz_filename=f"anchored_traversals_{intermediate_root_node_label}_{intermediate_root_node_property_value}_{get_time()}.html",
        )
        print(f"Uploading visualization html to s3 bucket {output_bucket} at {upload_folder_path}...")
        file_ul(bucket=output_bucket,
                output_folder=upload_folder_path,
                sub_folder="",
                newfile=viz_file)
    
    print("Anchored traversals flow completed.")
    return None

        
