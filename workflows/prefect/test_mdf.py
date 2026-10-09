from bento_mdf import MDFReader, MDFDataValidator
from meval.utils import (
    parse_file_url,
    get_time,
)
from prefect import flow, get_run_logger
from typing import Literal
from upsert_workflow import file_dl, file_ul


@flow(
        name="test_mdf_with_edp",
        log_prints=True
)
def test_mdf_with_edp(
        s3_uri_of_model_file: str,
        s3_uri_of_props_file: str,
        if_use_edp: Literal[True, False],
        output_s3_bucket_loc: str
) -> None:
    runner_logger = get_run_logger()
    runner_logger.info("Downloading model and props files from s3 bucket")

    model_bucket, model_filepath = parse_file_url(s3_uri_of_model_file)
    props_bucket, props_filepath = parse_file_url(s3_uri_of_props_file)

    file_dl(model_bucket, model_filepath)
    file_dl(props_bucket, props_filepath)

    STS_URL = "https://sts.cancer.gov/v2"

    if if_use_edp:
        runner_logger.info("Resolving EDP for the mdf instance")
        mdf_instance = MDFReader(model_filepath, props_filepath, resolve_edps=if_use_edp, sts_url=STS_URL)
    else:
        runner_logger.info("Not resolving EDP for the mdf instance")
        mdf_instance = MDFReader(model_filepath, props_filepath)

    val_instance = MDFDataValidator(mdf_instance)
    runner_logger.info("Validation instance created successfully")

    # val yml file
    output_filename="pydantics_model.yml"
    with open(output_filename, "w") as f:
        f.write(val_instance.data_model)
    runner_logger.info(f"pydantic model YAML output written to {output_filename}")
    
    #upload output to s3 bucket location
    output_bucket, output_path = parse_file_url(output_s3_bucket_loc)
    subfolder_name = f"edp_test_{get_time()}"
    file_ul(bucket=output_bucket, output_folder=output_path, sub_folder=subfolder_name, newfile=output_filename)
    runner_logger.info(f"Uploaded output file {output_filename} to s3 location {output_s3_bucket_loc}")

