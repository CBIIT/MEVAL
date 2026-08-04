import sys
import os
import json
from meval import Validator
from add_uuid_to_files import add_uuid_to_files
from upsert_workflow import get_secret_task, file_ul
from prefect import flow, get_run_logger, pause_flow_run, task
from prefect.input import RunInput
from meval.utils import parse_file_url, get_time, folder_dl_s3
from bento_mdf import MDFReader
from typing import Literal, TypeVar
from neo4j import GraphDatabase

sys.path.insert(0, os.path.abspath("./libs/prefect-toolkit"))
from workflow.validate_submission import download_model_files

UuidDropDownChoices = Literal["yes", "no"]
ProjectDropDownChoices = Literal["ccdi", "icdc", "cds", "c3dc", "ctdc", "ccdi_dcc", "popsci"]


class GenerateUuidInput(RunInput):
    subgraph_value: str
    id_prop_name: str = "guid"
    delimiter: str = ";"

class ValidationInput(RunInput):
    id_prop_name: str = "guid"
    delimiter : str = ";"

def find_newly_generated_tsv_files(folder_path: str) -> list[str]:
    """
    Find newly generated tsv files in the given folder path. Newly generated tsv files are those that end with "_added.tsv".

    Args:
        folder_path (str): The path to the folder where the tsv files are located.

    Returns:
        list[str]: A list of paths to the newly generated tsv files.
    """
    newly_generated_files = []
    for file_name in os.listdir(folder_path):
        if file_name.endswith("_added.tsv"):
            newly_generated_files.append(os.path.join(folder_path, file_name))
    return newly_generated_files

@flow(
    name="Validate submission files against database",
    log_prints=True,
    flow_run_name="{runner}-" + f"{get_time()}",
)
def validate_submission_against_db(
    tsv_folder_bucket_path: str,
    validation_output_bucket_path: str,
    db_account_id: str,
    db_creds_secret_name: str,
    uri_secret_key: str,
    commons_acronym: ProjectDropDownChoices,
    tag: str = "",
    does_file_contain_uuid: UuidDropDownChoices = "no",
    validation_mode: Literal["Upsert", "New", "Update"] = "Upsert",
    username_secret_key: str | None = None,
    password_secret_key: str | None = None,
) -> str:
    """
    Prefect workflow that validates the current DB content and flags any
    unexpected results.
    The validation mode controls which checks are run. For example, if the validation mode is "New", the validation will check if the records in the submission files are Truly new to DB.
    
        Args:
        tsv_folder_bucket_path (str): S3 bucket path to the folder containing the TSV files to be validated.
        validation_output_bucket_path (str): S3 bucket path to the folder where the validation output will be stored.
        does_file_contain_uuid (UuidDropDownChoices, optional): Indicates whether the submission files contain UUIDs. Defaults to "no".
        validation_mode (Literal["Upsert", "New", "Update"], optional): The mode of validation to be performed. Defaults to "Upsert".

        Returns:
            str: validation output filename
    """
    logger = get_run_logger()
    logger.info(f"Starting validation for files in {tsv_folder_bucket_path} with mode {validation_mode}")

    output_bucket, output_key_prefix = parse_file_url(validation_output_bucket_path)
    output_subfolder = f"validation_against_db_{get_time()}"

    # If the files do not contain UUIDs, add them
    if does_file_contain_uuid == "no":
        logger.info(
            "You indicated that the submission files DO NOT contain UUIDs. Adding UUID is needed before validation because UUIDs are used to uniquely identify records in the database"
        )
        generate_uuid = pause_flow_run(
            wait_for_input=GenerateUuidInput.with_initial_data(description=(f"""
**Please provide a subgraph value for your submission file**
Subgraph value is a string that indicates which study or program of these files belong to, for example, "phs000123". It means all the records from your submission files are from study phs000123.
Subgraph value will be used to generate UUIDs for records along with the project acronym and the record type,and record key prop value.

- **subgraph_value**: e.g., phs000123
- **id_prop_name**: the name of the UUID property, default is "guid". Please keep this property name consistent with what's been ingested in DB.
- **delimiter**: the delimiter used in the TSV files, default is ";"
"""))
        )
        delimiter = generate_uuid.delimiter
        id_prop_name = generate_uuid.id_prop_name
        # tsv with uuid added upload path
        tsv_with_uuid_added_upload_path = os.path.join(
            validation_output_bucket_path, output_subfolder
        )

        # This will create a bunch of file with filename ending in _added.tsv
        # newly generated tsv files will be uploaded to the output bucket loc, subfolder uuid_added_{timestamp}
        add_uuid_to_files(
            output_bucket_loc=tsv_with_uuid_added_upload_path,
            tsv_folder_s3uri=tsv_folder_bucket_path,
            subgraph_value=generate_uuid.subgraph_value,
            commons_acronym=commons_acronym,
            tag=tag,
            uuid_col_name=id_prop_name,
            delimiter=delimiter
        )
        submission_file_set = find_newly_generated_tsv_files(".")

    elif does_file_contain_uuid == "yes":
        validation_param_input = pause_flow_run(
            wait_for_input=ValidationInput.with_initial_data(description=(f"""
**Please provide id property name and delimiter used in your submission files**

- **id_prop_name**: the name of the UUID property, default is "guid". Please keep this property name consistent with what's been ingested in DB.
- **delimiter**: the delimiter used in the TSV files, default is ";"
""")
        ))
        logger.info(
            "You indicated that the submission files already contain UUIDs. Proceeding with validation."
        )
        delimiter = validation_param_input.delimiter
        id_prop_name = validation_param_input.id_prop_name

        # Parse the S3 bucket path to get the folder path
        tsv_bucket, tsv_folder_path = parse_file_url(tsv_folder_bucket_path)
        folder_dl_s3(bucket=tsv_bucket, remote_folder=tsv_folder_path)
        # tsv files are downlaoded at folder path of tsv_folder_path
        submission_file_set = [os.path.join(tsv_folder_path, i) for i in os.listdir(tsv_folder_path) if i.endswith(".tsv")]
    else:
        raise ValueError(
            f"Invalid value for does_file_contain_uuid: {does_file_contain_uuid}. Must be 'yes' or 'no'."
        )

    # download data model files and create MDFReader instance
    data_model_yaml, props_yaml = download_model_files(
            commons_acronym=commons_acronym, tag=tag
        )
    logger.info(
        f"Downloaded data model, props yaml: {data_model_yaml}, {props_yaml}"
    )
    logger.info(f"Downloaded data model yaml: {data_model_yaml}")
    logger.info(f"Downloaded properties yaml: {props_yaml}")
    mdf_instance = MDFReader(data_model_yaml, props_yaml, handle=commons_acronym)
    logger.info("Created MDFReader instance for data model features reading")

    # driver instance for db connection
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

    validation_result = {}
    for tsv_file in submission_file_set:
        logger.info(f"Validating file: {os.path.basename(tsv_file)} against the database")
        file_validation = Validator.validate_tsv_in_db(
            driver=driver,
            tsv_file_path=tsv_file,
            tsv_file_set=submission_file_set,
            mdf_instance=mdf_instance,
            id_prop_name=id_prop_name,
            delimiter=delimiter,
            validation_mode=validation_mode
        )
        validation_result[os.path.basename(tsv_file)] = file_validation

    # write vlaidation result to a json file and upload to s3 bucket
    validation_output_filename = f"validation_against_db_result_{get_time()}.json"
    with open(validation_output_filename, "w") as f:
        json.dump(validation_result, f, indent=4)
    logger.info(f"Validation result written to {validation_output_filename}")

    # upload the validation result json file to s3 bucket
    # upload the log file to s3
    file_ul(
        bucket=output_bucket,
        output_folder=output_key_prefix,
        sub_folder=output_subfolder,
        newfile=validation_output_filename,
    )
    logger.info(f"Validation result uploaded to s3 bucket {output_bucket} at {output_key_prefix}/{output_subfolder}/{validation_output_filename}")
    logger.info("Validation workflow completed")
