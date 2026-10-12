from bento_mdf import MDFReader, MDFDataValidator
from meval.utils import (
    parse_file_url,
    get_time,
)
from prefect import flow, get_run_logger
from typing import Literal
from upsert_workflow import file_dl, file_ul


@flow(
        name="get_pydantic_model_for_validation",
        log_prints=True
)
def get_pydantic_model(
        s3_uri_of_model_file: str,
        s3_uri_of_props_file: str,
        if_resolve_edp: Literal[True, False],
        output_s3_bucket_loc: str
) -> None:
    """Generates the pydantic model YAML for validation from the given model and props files.

    Args:
        s3_uri_of_model_file (str): S3 URI of the model file.
        s3_uri_of_props_file (str): S3 URI of the props file.
        if_resolve_edp (Literal[True, False]): Whether to resolve EDPs for the MDF instance.
        output_s3_bucket_loc (str): S3 bucket location to upload the generated pydantic model YAML.
    """
    runner_logger = get_run_logger()
    runner_logger.info("Downloading model and props files from s3 bucket")

    model_bucket, model_filepath = parse_file_url(s3_uri_of_model_file)
    props_bucket, props_filepath = parse_file_url(s3_uri_of_props_file)

    model_file=file_dl(model_bucket, model_filepath)
    props_file=file_dl(props_bucket, props_filepath)

    STS_URL = "https://sts.cancer.gov/v2"

    if if_resolve_edp:
        runner_logger.info("Resolving EDP for the mdf instance")
        mdf_instance = MDFReader(model_file, props_file, resolve_edps=if_resolve_edp, sts_url=STS_URL)
    else:
        runner_logger.info("Not resolving EDP for the mdf instance")
        mdf_instance = MDFReader(model_file, props_file)

    val_instance = MDFDataValidator(mdf_instance)
    runner_logger.info("Validation instance created successfully")

    # val yml file
    output_filename="pydantic_model.yml"
    with open(output_filename, "w") as f:
        f.write(val_instance.data_model)
    runner_logger.info(f"pydantic model YAML output written to {output_filename}")
    
    #upload output to s3 bucket location
    output_bucket, output_path = parse_file_url(output_s3_bucket_loc)
    subfolder_name = f"get_pydantic_model_{get_time()}"
    file_ul(bucket=output_bucket, output_folder=output_path, sub_folder=subfolder_name, newfile=output_filename)
    runner_logger.info(f"Uploaded output file {output_filename} to s3 location {output_s3_bucket_loc}")

