from datetime import datetime
from prefect import flow
import time
import json
from contextlib import contextmanager
from src.loader import Loader
from neo4j import GraphDatabase
from workflows.prefect.upsert_workflow import get_secret_task


@contextmanager
def timer(label: str=""):
    start = time.perf_counter()
    try:
        yield
    finally:
        end = time.perf_counter()
        duration = end - start
        print(f"{label} took {duration:.3f} seconds")


@flow(log_prints=True, name="Delete Database Study Subgraph Flow")
def delete_database_study_subgraph(
    db_creds_secret_name: str,
    uri_secret_key: str,
    root_node_label: str,
    root_node_property: str,
    root_node_property_value: list[str],
    username_secret_key: str | None = None,
    password_secret_key: str | None = None,
):
    """
    Delete study subgraphs in the database by matching a root node and removing all connected nodes and relationships.

    Args:
        db_creds_secret_name (str): The name of the AWS Secrets Manager secret containing database credentials.
        uri_secret_key (str): The key for the database URI in the secret.
        root_node_label (str): The label of the root node. It is the origin from which all other nodes in its tree structure descend. Root node has no other parent node it points to. In most cases, this root node label is 'study'
        root_node_property (str): The property name of the root node to match. In some cases, this property is 'study_id'.
        root_node_property_value (list[str]): A list of the property values of the root node to match. This can be a list of study/dbGaP accession ids, such as 'phs000123'
        username_secret_key (str | None): The key for the database username in the secret.
        password_secret_key (str | None): The key for the database password in the secret.
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
    # create myloader instance
    myloader = Loader(driver=driver)
    deletion_summary = {}
    for value in root_node_property_value:
        print(f"Preparing to delete subgraph with root node {root_node_label} of {root_node_property} = {value}...")
        with timer("Deleting subgraph"):
            subgraph_deletion_summary = myloader.wipe_subgraph(root_node_label=root_node_label, root_node_prop=root_node_property, subgraph_value=value)
            deletion_summary[value] = subgraph_deletion_summary
        print(f"Completed deletion of subgraph with root node {root_node_label} of {root_node_property} = {value}. Summary: {subgraph_deletion_summary}")
    print("Summary of all deletions:", json.dumps(deletion_summary, indent=2))
    return None
