from prefect import flow, task, get_run_logger
from datetime import datetime
from src.validator import Validator
from bento_mdf import MDFReader
import os
import pandas as pd
import json
from typing import Literal
from src.utils import parse_file_url
import logging
from upsert_workflow import get_secret_task, file_dl, folder_dl, file_ul, get_logger
import sys
sys.path.insert(0, os.path.abspath("./libs/prefect-toolkit"))
from workflow.validate_submission import download_model_files

DropDownChoices = Literal["ccdi", "icdc", "cds", "c3dc", "ctdc", "ccdi_dcc"]

@flow(name="Validate TSV Files")
def validate_tsv_files(
    output_bucket_loc: str,
    tsv_folder_s3uri: str,
    commons_acronym: DropDownChoices,
    tag: str = "",
    delimiter: str = ";"
)-> None:
    """
    Validates TSV files in a specified S3 location using the Validator class.
    This workflow will validate 

    Args:
        output_bucket_loc (str): The S3 URI of the output bucket where validation results will be stored.
        tsv_folder_s3uri (str): The S3 URI of the folder containing the TSV files to be validated.
        commons_acronym (DropDownChoices): The acronym of the commons for which the TSV files are being validated.
        tag (str, optional): An optional tag to append to the output file name. Defaults to "".
        delimiter (str, optional): The delimiter used in the TSV files. Defaults to ";".
    """
    flow_logger = get_run_logger()
    file_logger_name = f"validate_tsv_files_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    file_logger = get_logger(file_logger_name)

    # output bucket and key prefix for validation results
    output_bucket, output_key_prefix = parse_file_url(output_bucket_loc)
    output_subfolder = "MEVAL_validation_summaries_" + datetime.now().strftime(
        "%Y%m%d_%H%M%S"
    )

    # Download model files from S3
    # test downloading model files
    data_model_yaml, props_yaml = download_model_files(
        commons_acronym=commons_acronym, tag=tag
    )
    file_logger.info(
        f"Downloaded data model, props yaml: {data_model_yaml}, {props_yaml}"
    )
    flow_logger.info(f"Downloaded data model yaml: {data_model_yaml}")
    flow_logger.info(f"Downloaded properties yaml: {props_yaml}")
    model_mdf = MDFReader(data_model_yaml, props_yaml, handle=commons_acronym)

    # Download TSV files from S3
    tsv_bucket, tsv_folder = parse_file_url(tsv_folder_s3uri)
    folder_dl(tsv_bucket, tsv_folder) # this will create a folder path of tsv_folder locally
    tsv_file_list = Validator.find_tsv_files(folder_path=tsv_folder) # this returns a list of PosixPath obj of all tsv files under tsv_folder
    tsv_file_str_list = [str(file) for file in tsv_file_list]
    flow_logger.info(f"Found TSV files under {tsv_folder}: {', '.join(tsv_file_str_list)}")

    # Validate tsv files
    validator = Validator(mdf=model_mdf)
    # validate tsv format, format_val_results is a dict with file path as key, and format error list as value.
    format_val_results = validator.validate_tsv_files_format(file_path_list=tsv_file_list)
    if len(format_val_results) > 0:
        flow_logger.warning(f"Format validation found issues in the following files: {', '.join(format_val_results.keys())}")
        file_logger.warning(f"Format validation found issues in the following files: {', '.join(format_val_results.keys())}")
        # write format validation results to as a json file
        format_val_filename = f"format_validation_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(format_val_filename, "w") as f:
            json.dump(format_val_results, f, indent=4)
        flow_logger.info(f"Format validation results written to {format_val_filename}")
        file_logger.info(f"Format validation results written to {format_val_filename}")
        # upload the format validation results to s3
        file_ul(
            bucket=output_bucket,
            output_folder=output_key_prefix,
            sub_folder=output_subfolder,
            newfile=format_val_filename,
        )
    else:
        flow_logger.info("All files passed tsv format validation.")
        file_logger.info("All files passed tsv format validation.")

    # we can only perform further validation if the the file passes the format validation
    format_valid_files =[file for file in tsv_file_str_list if file not in format_val_results]
    flow_logger.info(f"A total of {len(format_valid_files)} files passed tsv format validation and will be further validated: {', '.join(format_valid_files)}")
    file_logger.info(f"A total of {len(format_valid_files)} files passed tsv format validation and will be further validated: {', '.join(format_valid_files)}")

    # validate records in tsv files that passed format validation
    if len(format_valid_files) > 0:
        record_val_results = {}
        file_logger.info(f"Start record validation for files")
        flow_logger.info(f"Start record validation for files")
        for file in format_valid_files:
            file_logger.info(f"Validating records in file {file}")
            flow_logger.info(f"Validating records in file {file}")
            file_record_val = validator.validate_tsv_records(file_path=file, sungraph_col=None, id_field=None, delimiter=delimiter)
            if len(file_record_val) > 0:
                flow_logger.warning(f"Found record issues in file {file}")
                file_logger.warning(f"Found record issues in file {file}")
                # write record validation results to as a json file
                record_val_results[file] = file_record_val
            else:
                record_val_results[file] = "Pass"
                flow_logger.info(f"All records in file {file} passed record validation.")
                file_logger.info(f"All records in file {file} passed record validation.")
        # write record validation results to a json file
        record_val_filename = f"record_validation_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(record_val_filename, "w") as f:
            json.dump(record_val_results, f, indent=4)
        flow_logger.info(f"Record validation results written to {record_val_filename}")
        file_logger.info(f"Record validation results written to {record_val_filename}")
        # upload the record validation results to s3
        file_ul(
            bucket=output_bucket,
            output_folder=output_key_prefix,
            sub_folder=output_subfolder,
            newfile=record_val_filename,
        )
        flow_logger.info("Record validation results uploaded to s3")
        file_logger.info("Record validation results uploaded to s3")
    

        # validate relationships between files that passed format validation
    else:
        flow_logger.info("No files passed tsv format validation, skipping further validations.")
        file_logger.info("No files passed tsv format validation, skipping further validations.")
