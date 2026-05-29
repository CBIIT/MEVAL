from prefect import flow, get_run_logger
from datetime import datetime
from meval.validator import Validator
from bento_mdf import MDFReader
import os
import pandas as pd
import json
from typing import Literal
from meval.utils import get_time, parse_file_url
from upsert_workflow import folder_dl, file_ul
import sys

sys.path.insert(0, os.path.abspath("./libs/prefect-toolkit"))
from workflow.validate_submission import download_model_files

DropDownChoices = Literal["ccdi", "icdc", "cds", "c3dc", "ctdc", "ccdi_dcc", "popsci"]


@flow(
    name="Add UUID to a set of TSV files",
    log_prints=True,
    flow_run_name="add_uuid_{commons_acronym}_{tag}_" + f"{get_time()}",
)
def add_uuid_to_files(
    output_bucket_loc: str,
    tsv_folder_s3uri: str,
    subgraph_value: str,
    commons_acronym: DropDownChoices,
    tag: str = "",
    uuid_col_name: str = "guid",
    delimiter: str = ";",
) -> None:
    """
    This flow is to generate uuid values under uuid_col_name column for a set of tsv files. This flow is also going to replace any linkage column with the uuid col, such participant.participant_id will be replaced with participant.guid
    We recommend running this flow after the set of submission studies (for a study or a program) have passed MEVAL validation pipeline.

    Args:
        output_bucket_loc (str): the s3 uri of the output bucket/folder where the new TSV files with uuid column will be uploaded
        tsv_folder_s3uri (str): the s3 uri of the input folder that contains the original TSV files without uuid column
        commons_acronym (DropDownChoices): the acronym of the commons, e.g. "ccdi", "icdc", "cds", "c3dc", "ctdc", "ccdi_dcc", "popsci"
        subgraph_value (str): a string that indicates which study or program of these files belong to, such as "phs000123"
        delimiter (str, optional): the delimiter used in the TSV files, default is ";"

    Returns:
        None
    """
    flow_logger = get_run_logger()
    upload_folder_name = f"uuid_added_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    # Download model files from github repo
    # test downloading model files
    data_model_yaml, props_yaml = download_model_files(
        commons_acronym=commons_acronym, tag=tag
    )
    model_mdf = MDFReader(data_model_yaml, props_yaml, handle=commons_acronym)

    # output bucket and key prefix for validation results
    output_bucket, output_key_prefix = parse_file_url(output_bucket_loc)

    # download tsv files from s3
    flow_logger.info(f"Downloading tsv files from s3 uri: {tsv_folder_s3uri}")
    tsv_bucket, tsv_folder = parse_file_url(tsv_folder_s3uri)
    folder_dl(
        tsv_bucket, tsv_folder
    )  # this will create a folder path of tsv_folder locally
    tsv_file_list = Validator.find_tsv_files(folder_path=tsv_folder)
    flow_logger.info(
        f"Found {len(tsv_file_list)} TSV files under {tsv_folder} after downloading from s3"
    )

    # add uuid column to each tsv file based on the specified columns in subgraph_dict
    for file in tsv_file_list:
        file_filename = os.path.basename(file)
        output_filename = file_filename.split(".tsv")[0] + f"_{uuid_col_name}_added.tsv"
        Validator.add_uuid_to_tsv_file(
            file_path=file,
            project_name=commons_acronym,
            mdf=model_mdf,
            output_file_path=output_filename,
            uuid_column=uuid_col_name,
            delimiter=delimiter,
            subgraph_value=subgraph_value,
        )
        # upload this new file with uuid column to s3
        file_ul(
            bucket=output_bucket,
            output_folder=output_key_prefix,
            sub_folder=upload_folder_name,
            newfile=output_filename,
        )
        flow_logger.info(
            f"Uploaded file {output_filename} to s3 location: {output_bucket_loc}"
        )

    return None
