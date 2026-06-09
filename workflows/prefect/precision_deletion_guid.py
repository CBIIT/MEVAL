from meval.loader import Loader
from neo4j import GraphDatabase
from meval.utils import (
    parse_file_url,
    get_time,
    is_valid_s3_uri,
    parse_file_url,
)
import json
from prefect import flow, task, get_run_logger
from workflows.prefect.upsert_workflow import get_secret_task, file_dl, file_ul

def uuid_input_validation(guid_input: list[str] | str) -> bool:
    """Validate the input for UUID values, ensuring it's either a list of strings or a valid file path.
    
    Args:        
        guid_input (list[str] | str): The input which can be a list of UUID strings or a file path containing the UUIDs.
    
    Returns:
        bool: True if the input is valid, False otherwise.
    """
    if isinstance(guid_input, list):
        if all(isinstance(item, str) for item in guid_input):
            if all(item != "" for item in guid_input):
                return True
            else:
                raise ValueError("List of UUIDs cannot contain empty strings.")
        else:
            raise ValueError("All items in the list must be strings.")
    elif isinstance(guid_input, str):
        if is_valid_s3_uri(guid_input):
            return True
        else:
            raise ValueError("Invalid S3 URI provided.")
    else:
        raise ValueError("Input must be a list of UUID in string format or a valid S3 URI.")


def read_string_list_file(filepath: str) -> list[str]:
    """Read uuid file

    Args:
        filepath (str): filepath to the file containing a list of strings (uuid)
    Raises:
        ValueError: If the file cannot be read, is not in valid JSON format, or does not contain a list of strings.

    Returns:
        list[str]: A list of strings (UUIDs) read from the file.
    """
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, list) and all(isinstance(x, str) for x in data) and all(x != "" for x in data):
                return data
            else:
                raise ValueError("File must contain a list of non-empty strings.")
    except (json.JSONDecodeError, OSError) as e:
        raise ValueError(f"File not found or invalid JSON format. {e}")  


@flow(
    name="Precision Deletion Nodes",
    log_prints=True,
    flow_run_name=lambda: f"precision_deletion_{get_time()}",
)
def precision_deletion_guid(
    db_account_id: str,
    db_creds_secret_name: str,
    uri_secret_key: str,
    output_bucket_loc: str,
    uuid_value_input: list[str] | str,
    uuid_property_name: str = "guid",
    dry_run: bool = True,
    username_secret_key: str | None = None,
    password_secret_key: str | None = None,
) -> None:
    """
    Flow to delete nodes based on a list of UUIDs (GUIDs) with a specified property name.

    Args:
        db_account_id (str): Database account ID for authentication.
        db_creds_secret_name (str): Name of the secret containing database credentials.
        uri_secret_key (str): Key name in the secret for the database URI.
        output_bucket_loc (str): S3 bucket location to store the output results.
        uuid_value_input (list[str] | str): Either a list of UUID strings or a file path (S3 URI) containing the UUIDs to be deleted. A file contains a list of UUIDs in JSON format, e.g. ["uuid1", "uuid2", ...].
        uuid_property_name (str, optional): The property name that holds the UUID value in the nodes. Defaults to "guid".
        dry_run (bool, optional): If True, will not perform actual deletion but will log the nodes that would be deleted. Defaults to True.
        username_secret_key (str | None, optional): Key in the secret for the database username, if applicable. Defaults to None.
        password_secret_key (str | None, optional): Key in the secret for the database password, if applicable. Defaults to None.
    
    Returns:
        None
    """
    logger = get_run_logger()

    # validate uuid_input.
    # The workflow fails if the input fails validation
    uuid_input_validation(uuid_value_input)

    # download uuid input if it is a file
    if isinstance(uuid_value_input, str): # the uuid_input_validation have checked it is a valid s3 uri if it is a string
        uuid_bucket, uuid_filepath = parse_file_url(uuid_value_input)
        uuid_file = file_dl(bucket=uuid_bucket, filename=uuid_filepath)
        guid_list = read_string_list_file(uuid_file)
    else: # guid_value_input is a list of strings
        guid_list = uuid_value_input
    logger.info(f"Total number of guid received: {len(guid_list)}")

    # get driver to connect to a database instance
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

    myloader = Loader(driver=driver)

    guid_inspections = []
    guid_to_delete = []
    # frist test if the guid exist in the database
    for guid in guid_list:
        logger.info(f"Checking guid: {guid} for uniqueness in the database")
        check_uniq, check_uniq_info = myloader.check_unique_node(property_name=uuid_property_name, property_value=guid)
        if not check_uniq:
            # two types of errors can happen: 1) node not found, then there is nothing to delete with guid, 2) multiple nodes were found that that guid, then there is a data issue in the database that requires additional investigation before deletion.
            # Both scenarios are not ideal for deletion.
            logger.error(
                f"Node with {uuid_property_name}={guid} either not exit or not unique.\n{json.dumps(check_uniq_info, indent=2, default=str)}"
            )
            guid_inspections.append(check_uniq_info) # add error information to the guid inspections

        else: # node passed uniqueness test
            logger.info(f"Checking guid:{guid} for upstream/children nodes.")
            # check if the node is a non-leaf node with incoming relationships
            guid_to_delete.append(guid) # guid is up for deltion
            upstream_nodes = myloader.find_upstream_nodes(property_value=guid, property_name=uuid_property_name)
            if len(upstream_nodes) > 0: # if children/upstream nodes found
                logger.warning(f"Node with {uuid_property_name}={guid} is NOT a leaf node. Found upstream/children nodes pointing to target node")
                # there is a change that children/upstream nodes already included in the guid_list
                for upstream_node in upstream_nodes:
                    if (
                        upstream_node["upstream_node"]["properties"][uuid_property_name]
                        not in guid_list
                    ):
                        logger.error(
                            f"Node with {uuid_property_name}={guid} has an upstream node not included in the deletion list. \nUpstream node info: \n{json.dumps(upstream_node, indent=2, default=str)}"
                        )
                        guid_inspections.append(upstream_node) # add error information to the guid inspections
                        guid_to_delete.append(upstream_node["upstream_node"]["properties"][uuid_property_name]) # add the guid of the upstream node to the guid_to_delete list, even if it is not in the original guid_list, because it is a child/upstream node of the target node, and it should be deleted together with the target node to avoid orphan nodes.
                    else:
                        # upstream_node already in the guid_list, no action needed
                        logger.info(
                            f"Node with {uuid_property_name}={guid} has an upstream node but that node is also included in the deletion list. \nUpstream node info: \n{json.dumps(upstream_node, indent=2, default=str)}"
                        )
                        # because it is already in the guid_list, no need to be added to the guid_to_delete
            else: # no children/upstream nodes found, it is safe to be deleted, guid is already added to the guid_to_delete
                logger.info(f"Node with {uuid_property_name}={guid} passed upstream/children node check. It is safe to be deleted without causing orphan nodes issue.")
                pass

    # print out guid_inspection, guid_to_delete list
    print(f"guid inspection list:\n{json.dumps(guid_inspections, indent=2, default=str)}")
    print(f"guid to delete list:\n{json.dumps(guid_to_delete, indent=2, default=str)}")

    # parse output bucket location
    output_bucket, output_folder = parse_file_url(output_bucket_loc)
    output_subfolder = f"precision_deletion_{get_time()}"

    if len(guid_inspections) > 0: # issues were found in the guid inspection, no deletioni will be performed even if dry_run is set to false
        logger.warning("We found potential issues with the guid(s) to be deleted. Please review the inspection results for details.")
        # upload guid inspection results to s3 for review
        inspection_output_file = f"guid_inspection_{get_time()}.json"
        with open(inspection_output_file, "w", encoding="utf-8") as f:
            json.dump(guid_inspections, f, indent=2, default=str)
        file_ul(bucket=output_bucket, output_folder=output_folder, sub_folder=output_subfolder, newfile=inspection_output_file)
        logger.warning(f"inspection results have been uploaded to {output_subfolder} under bucket {output_bucket_loc} for review")

        # upload guid_to_delete list to s3 for review
        if len(guid_to_delete) > 0:
            guid_to_delete_output_file = f"guid_ready_to_delete_{get_time()}.json"
            with open(guid_to_delete_output_file, "w", encoding="utf-8") as f:
                json.dump(guid_to_delete, f, indent=2, default=str)
            file_ul(bucket=output_bucket, output_folder=output_folder, sub_folder=output_subfolder, newfile=guid_to_delete_output_file)
            logger.info(f"guid ready to delete list has been uploaded to {output_subfolder} under bucket {output_bucket_loc} for review")
            logger.warning("guid ready to delete list DOES NOT contain any guid that does not exist or not unique in the database. The list ONLY contains guids that passed uniquness test (from the provided guid list) and any potential upstream/children nodes of the provided guids")
        else: # guid_to_delete is empty, no guid passed the inspection, no deletion will be performed
            logger.info("guid ready to delete list is empty. No guid passed the inspection, no deletion will be performed.")

    else: # guid_inspection is empty, no issue found with provided guid. It is safe to proceed with deletion if dry run is not enabled.
        logger.info("No potential issue found with the guid(s) to be deleted based on our checks.")
        if dry_run:
            logger.info(f"Dry run enabled. No data nodes will be deleted.")
        else:
            logger.info(f"Dry run disabled. The nodes provided ({len(guid_to_delete)}) will be deleted")
            total_deleted = myloader.delete_nodes_by_prop_value(identifier_list=guid_to_delete, property_name=uuid_property_name)
            logger.info(f"Total nodes deleted: {total_deleted}")

    logger.info("Precision deletion flow completed.")
