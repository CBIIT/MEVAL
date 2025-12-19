from datetime import datetime
from prefect import flow
from src.loader import Loader
from neo4j import GraphDatabase
from workflows.prefect.upsert_workflow import get_secret_task

@flow(log_prints=True, name="Wipe out Database Flow")
def wipe_database(
    db_creds_secret_name: str,
    uri_secret_key: str,
    username_secret_key: str | None = None,
    password_secret_key: str | None = None,
):
    """
    Wipe out the graph database by deleting all nodes and relationships.

    Args:
        db_creds_secret_name (str): The name of the AWS Secrets Manager secret containing database credentials.
        uri_secret_key (str): The key for the database URI in the secret.
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

    driver = GraphDatabase.driver(uri, auth=(username, password) if username and password else None)

    myloader = Loader(driver=driver)
    myloader.wipe_database()
    print("Wiping database completed.")
    return None
