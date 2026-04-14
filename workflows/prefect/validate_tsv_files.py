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

DropDownChoices = Literal["ccdi", "icdc", "cds", "c3dc", "ctdc", "ccdi_dcc", "popsci"]

@flow(name="Validate TSV Files")
def validate_tsv_files(
    output_bucket_loc: str,
    tsv_folder_s3uri: str,
    commons_acronym: DropDownChoices,
    tag: str = "",
    delimiter: str = ";"
)-> None:
    """
    Validates a set of TSV files for a study or a program using the Validator class. Please include all the files of a study/program when running the validation pipeline as partial files may have data issues overlooked.
    Validation pipeline includes: tsv format checking, record validation, relationship validation, and uniqueness validation.

    Args:
        output_bucket_loc (str): The S3 URI of the output bucket where validation results will be stored.
        tsv_folder_s3uri (str): The S3 URI of the folder containing the TSV files to be validated.
        commons_acronym (DropDownChoices): The acronym of the commons for which the TSV files are being validated.
        tag (str, optional): An optional tag to append to the output file name. Defaults to "".
        delimiter (str, optional): The delimiter used in the TSV files. Defaults to ";".
    """
    flow_logger = get_run_logger()
    file_logger_name = f"MEVAL_validation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    file_logger = get_logger(file_logger_name)

    # output bucket and key prefix for validation results
    output_bucket, output_key_prefix = parse_file_url(output_bucket_loc)
    output_subfolder = "MEVAL_validation_summaries_" + datetime.now().strftime(
        "%Y%m%d_%H%M%S"
    )

    # Download model files from github model repo
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
    flow_logger.info(f"Found {len(tsv_file_str_list)} TSV files under {tsv_folder}")
    file_logger.info(f"Found {len(tsv_file_str_list)} TSV files under {tsv_folder}")

    # Validate tsv files
    validator = Validator(mdf=model_mdf)
    # validate tsv format, format_val_results is a dict with file path as key, and format error list as value.
    format_val_results = validator.validate_tsv_files_format(file_path_list=tsv_file_list)
    if len(format_val_results) > 0:
        flow_logger.error(f"Format validation found issues in the following files: {', '.join(format_val_results.keys())}")
        file_logger.error(f"Format validation found issues in the following files: {', '.join(format_val_results.keys())}")
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
            sub_folder=output_subfolder + "/validation_results",
            newfile=format_val_filename,
        )
    else:
        flow_logger.info("All files passed tsv format validation.")
        file_logger.info("All files passed tsv format validation.")

    # we can only perform further validation if the the file passes the format validation
    format_valid_files =[file for file in tsv_file_str_list if file not in format_val_results]
    flow_logger.info(f"A total of {len(format_valid_files)} files passed tsv format validation and will be further validated")
    file_logger.info(f"A total of {len(format_valid_files)} files passed tsv format validation and will be further validated")

    # validate records in tsv files that passed format validation
    if len(format_valid_files) > 0:
        # Validate records in tsv files that passed format validation
        record_val_results = {}
        file_logger.info(f"Start record validation for files")
        flow_logger.info(f"Start record validation for files")
        for file in format_valid_files:
            file_logger.info(f"Validating records in file {file}")
            flow_logger.info(f"Validating records in file {file}")
            file_record_val = validator.validate_tsv_records(file_path=file, subgraph_col=None, id_field=None, delimiter=delimiter)
            if len(file_record_val) > 0:
                flow_logger.warning(f"Found record issues in file {file}")
                file_logger.warning(f"Found record issues in file {file}")
                # write record validation results to as a json file
                record_val_results[file] = file_record_val
            else:
                # record_val_results[file] = "Pass"
                flow_logger.info(f"All records in file {file} passed record validation.")
                file_logger.info(f"All records in file {file} passed record validation.")
        
        # if record_val_results is not empty, it means record issues were found
        if len(record_val_results) > 0:
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
                sub_folder=output_subfolder + "/validation_results",
                newfile=record_val_filename,
            )
            flow_logger.info("Record validation results uploaded to s3")
            file_logger.info("Record validation results uploaded to s3")
        else:
            # no record issue found across all files, no record validation results uploaded, just log the status
            flow_logger.info("All records in all files passed record validation.")
            file_logger.info("All records in all files passed record validation.")

        # validate relationships between files that passed format validation
        flow_logger.info(f"Start relationship validation for files")
        file_logger.info(f"Start relationship validation for files")
        rel_val_results = validator.validate_tsv_rels(file_path_list=format_valid_files, rel_delimiter=delimiter)
        # find files wth relationship issues
        files_with_rel_issues = list(rel_val_results.keys())
        if len(files_with_rel_issues)>0:
            flow_logger.warning(f"Found relationship issues in the following files: {', '.join(files_with_rel_issues)}")
            file_logger.warning(f"Found relationship issues in the following files: {', '.join(files_with_rel_issues)}")
            rel_val_filename = f"relationship_validation_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(rel_val_filename, "w") as f:
                json.dump(rel_val_results, f, indent=4)
            flow_logger.info(f"Relationship validation results written to {rel_val_filename}")
            file_logger.info(f"Relationship validation results written to {rel_val_filename}")
            # upload the relationship validation results to s3
            file_ul(
                bucket=output_bucket,
                output_folder=output_key_prefix,
                sub_folder=output_subfolder + "/validation_results",
                newfile=rel_val_filename,
            )
            flow_logger.info("Relationship validation results uploaded to s3")
            file_logger.info("Relationship validation results uploaded to s3")
        else:
            # no rel issue found, no val results uploaded, just log the pass status
            flow_logger.info("All relationships between files passed relationship validation.")
            file_logger.info("All relationships between files passed relationship validation.")

        # validation uniq entry based off key properties
        flow_logger.info(f"Start unique entry validation for files")
        file_logger.info(f"Start unique entry validation for files")
        uniq_entry_val_results = validator.validate_tsv_uniq_entry(file_path_list=format_valid_files)
        if len(uniq_entry_val_results) > 0:
            flow_logger.warning(f"Found duplicate entries based on key properties: {len(uniq_entry_val_results)} entries.")
            file_logger.warning(f"Found duplicate entries based on key properties: {len(uniq_entry_val_results)} entries.")
            uniq_entry_val_filename = f"uniq_entry_validation_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(uniq_entry_val_filename, "w") as f:
                json.dump(uniq_entry_val_results, f, indent=4)
            flow_logger.info(f"Unique entry validation results written to {uniq_entry_val_filename}")
            file_logger.info(f"Unique entry validation results written to {uniq_entry_val_filename}")
            # upload the unique entry validation results to s3
            file_ul(
                bucket=output_bucket,
                output_folder=output_key_prefix,
                sub_folder=output_subfolder + "/validation_results",
                newfile=uniq_entry_val_filename,
            )
            flow_logger.info("Unique entry validation results uploaded to s3")
            file_logger.info("Unique entry validation results uploaded to s3")
        else:
            # no uniq entry issue found, no val results uploaded, just log the pass status
            flow_logger.info("All entries in files passed unique entry validation")
            file_logger.info("All entries in files passed unique entry validation")
    else:
        flow_logger.warning("No files passed tsv format validation, skipping further validations.")
        file_logger.warning("No files passed tsv format validation, skipping further validations.")

    # validation pipeline completes
    flow_logger.info("TSV validation pipeline completed.")
    file_logger.info("TSV validation pipeline completed.")
    # upload the log file to s3
    file_ul(
        bucket=output_bucket,
        output_folder=output_key_prefix,
        sub_folder=output_subfolder,
        newfile=file_logger_name,
    )
    flow_logger.info("Log file uploaded to s3")
