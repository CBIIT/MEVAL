import sys
import os
import json
from meval import DatabaseValidator
from upsert_workflow import get_secret_task, file_ul
from prefect import flow, get_run_logger
from meval.utils import parse_file_url, get_time
from bento_mdf import MDFReader
from neo4j import GraphDatabase
from workflows.prefect.validate_submission_against_db import ProjectDropDownChoices

sys.path.insert(0, os.path.abspath("./libs/prefect-toolkit"))
from workflow.validate_submission import download_model_files


@flow(name="Validate graph database records", log_prints=True)
def validate_db_records(
    output_bucket_path: str,
    db_account_id: str,
    db_creds_secret_name: str,
    uri_secret_key: str,
    commons_acronym: ProjectDropDownChoices,
    tag: str = "",
    uuid_prop_name: str = "guid",
    is_uuid_in_model: bool = True,
    username_secret_key: str | None = None,
    password_secret_key: str | None = None,
):
    """This flow inspects every data node in a graph database and validate against a MDF instance

    Args:
        output_bucket_path (str): The path to the output bucket where validation results will be stored.
        db_account_id: The AWS account ID where the database credentials are stored.
        db_creds_secret_name: The name path of the secret in AWS Secrets Manager that contains the database credentials.
        uri_secret_key: The key name in the secret that contains the database URI.
        commons_acronym (ProjectDropDownChoices): The acronym of the commons project.
        tag (str, optional): A tag to associate with the validation run. Defaults to "".
        uuid_prop_name (str, optional): The name of the UUID property in the MDF instance. Defaults to "guid".
        is_uuid_in_model (bool, optional): Whether the UUID is present in the model files. Defaults to True.
        username_secret_key (str | None, optional): The key name in the secret that contains the database username. Defaults to None.
        password_secret_key (str | None, optional): The key name in the secret that contains the database password. Defaults to None.
    """
    logger = get_run_logger()

    output_bucket, output_key_prefix = parse_file_url(output_bucket_path)
    output_subfolder = f"validation_db_records_{get_time()}"

    # download data model files and create MDFReader instance
    data_model_yaml, props_yaml = download_model_files(
            commons_acronym=commons_acronym, tag=tag
        )
    logger.info(
        f"Downloaded data model, props yaml: {data_model_yaml}, {props_yaml}"
    )
    mdf_instance = MDFReader(data_model_yaml, props_yaml, handle=commons_acronym)
    logger.info("Created MDFReader instance for data model features reading")
    val_instance = DatabaseValidator(mdf_instance)
    logger.info("Created DatabaseValidator instance for validation against db")

    # create driver instance
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
    logger.info("Created a driver instance for db connection")

    # start validating db records
    val_results = val_instance.validate_db_records(
        driver=driver,
        batch_size=10000,
        uuid_property=uuid_prop_name,
        uuid_in_model=is_uuid_in_model
    )
    val_raw_results_filename = "db_records_validation_raw_results.json"
    with open(val_raw_results_filename, "w") as f:
        json.dump(val_results, f)
    basic_val_matrix = val_instance.get_basic_val_matrix(val_results)
    basic_val_matrix.to_csv("db_records_validation_summary_table.tsv", sep="\t", index=False)
    logger.info("Saved basic validation matrix to db_records_validation_summary_table.tsv")
    val_simplified = val_instance.val_results_simplified(val_raw_results_filename)
    val_simplified_filename = "db_records_validation_summary.json"
    with open(val_simplified_filename, "w") as f:
        json.dump(val_simplified, f)
    logger.info("Saved simplified validation results to db_records_validation_summary.json")

    # upload validation summary files to the designated output bucket loc
    file_ul(
        bucket=output_bucket,
        output_folder=output_key_prefix,
        sub_folder=output_subfolder,
        newfile="db_records_validation_summary_table.tsv",
    )
    logger.info("Uploaded db_records_validation_summary_table.tsv to the output bucket")
    file_ul(
        bucket=output_bucket,
        output_folder=output_key_prefix,
        sub_folder=output_subfolder,
        newfile="db_records_validation_summary.json",
    )
    logger.info("Uploaded db_records_validation_summary.json to the output bucket")

    return None
