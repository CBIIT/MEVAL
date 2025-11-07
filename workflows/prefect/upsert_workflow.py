from prefect import flow, task
from src.loader import Loader
from src.parser import ModelParser
from neo4j import GraphDatabase
import os
import sys
import json
from datetime import date

sys.path.insert(0, os.path.abspath("../ChildhoodCancerDataInitiative-Prefect_Pipeline/src/"))
from utils import file_dl, folder_dl, parse_file_url


driver = GraphDatabase.driver("bolt+ssc://mdb.ctos-data-team.org:9667/")
myloader = Loader(driver=driver)
ccdi_parser = ModelParser(
    "/Users/liuq14/CTOS_DATATEAM/DATATEAM-280_universal_data_loader_spike/ccdi-model-3.1.0/model-desc/ccdi-model.yml",
    "/Users/liuq14/CTOS_DATATEAM/DATATEAM-280_universal_data_loader_spike/ccdi-model-3.1.0/model-desc/ccdi-model-props.yml",
    handle="ccdi",
)


@flow(log_print=True)
def upsert_files(
    uri: str,
    tsv_folder_s3uri: str,
    id_field: str,
    model_file_s3uri: str,
    props_file_s3uri: str,
    subgraph_col: str = None,
    username: str = None,
    password: str = None,
):
    """
    Upsert study data from TSV files located in the specified S3 URI into the Neo4j database.

    Args:
        uri (str): The Neo4j database URI.
        tsv_folder_s3uri (str): The S3 URI of the folder containing TSV files.
        id_field (str): The field to use as the unique identifier for nodes.
        model_file_s3uri (str): The S3 URI of the model yaml file.
        props_file_s3uri (str): The S3 URI of the properties yaml file.
        subgraph_col (str, optional): The column indicating subgraph information. Defaults to None.
        username (str, optional): Username for Neo4j authentication. Defaults to None.
        password (str, optional): Password for Neo4j authentication. Defaults to None.
    """
    # create a loader instance
    if username is not None and password is not None:
        driver = GraphDatabase.driver(uri, auth=(username, password))
    else:
        driver = GraphDatabase.driver(uri)
    myloader = Loader(driver=driver)

    # download model file and props file
    model_file_bucket, model_file_key = parse_file_url(model_file_s3uri)
    props_file_bucket, props_file_key = parse_file_url(props_file_s3uri)

    # download the files
    model_file_name = file_dl(model_file_bucket, model_file_key)
    props_file_name = file_dl(props_file_bucket, props_file_key)

    # create model parser
    model_parser = ModelParser(
        model_file=model_file_name,
        props_file=props_file_name,
        handle="handle",
    )

    # download tsv folder
    tsv_bucket, tsv_folder = parse_file_url(tsv_folder_s3uri)
    folder_dl(tsv_bucket, tsv_folder)
    file_list = [
        os.path.join(tsv_folder, f)
        for f in os.listdir(tsv_folder)
        if f.endswith(".tsv")
    ]

    # upsert tsv files
    # first to load all the nodes
    node_upsert_summary = {}
    for file in file_list:
        node_upsert_summary[file] = myloader.upsert_file_records(
            file_path=file, id_field=id_field, subgraph_col=subgraph_col
        )
    print(json.dumps(node_upsert_summary, indent=4))

    # second to load all the relationships
    rel_upsert_summary = {}
    for file in file_list:
        rel_upsert_summary[file] = myloader.upsert_file_relationships(
            file_path=file, model_parser=model_parser, id_field=id_field
        )
    print(json.dumps(rel_upsert_summary, indent=4))
