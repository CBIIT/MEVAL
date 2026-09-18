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
from workflows.prefect.find_db_floating_nodes import (
    find_floating_db_nodes_flow,
    delete_nodes_by_internal_id_flow,
    find_json_length,
    extract_internal_id_from_json,
)

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
    root_node_label: str,
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
        root_node_label (str): The label of the root node which doesn't have OUTGOING relationship to other nodes. Common root node labels are "study", "program", depending on the data model of each project.
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

    # check if all guid value are unique in the list
    # remove duplicate GUIDs from the list
    guid_list = list(set(guid_list))
    logger.info(f"Total number of unique guid after removing duplicates: {len(guid_list)}")

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

    guid_inspections = {}
    guid_to_delete = []
    error_count = 0 # keep track of number of issues fund in a giben guid list

    # first test if the guid exist in the database
    guids_passed_uniq = []
    if len(guid_list) > 5000:
        logger.warning(f"Large number of GUIDs received: {len(guid_list)}. The workflow will process in multiple batches.")
        # Consider splitting the guid_list into smaller batches for processing
        guid_batches = [guid_list[i:i + 5000] for i in range(0, len(guid_list), 5000)]
        for batch in guid_batches:
            logger.info(f"Processing batch of size: {len(batch)}")
            uniq_check_results = myloader.check_unique_nodes(property_name=uuid_property_name, property_values=batch)
            # Process the results for this batch
            for guid, (is_unique, info_dict) in uniq_check_results.items():
                guid_inspections[guid] = []
                guid_inspections[guid].append(info_dict)
                if not is_unique:
                    error_count += 1
                    logger.error(
                        f"Node with {uuid_property_name}={guid} either not exit or not unique.\n{json.dumps(info_dict, indent=2, default=str)}"
                    )
                else:
                    guid_to_delete.append(guid)
                    guids_passed_uniq.append(guid)
    else:
        uniq_check_results = myloader.check_unique_nodes(property_name=uuid_property_name, property_values=guid_list)
        # uniq_check_results if dict: {"guid1":(bool, info_dict)}
        for guid, (is_unique, info_dict) in uniq_check_results.items():
            guid_inspections[guid] = []
            guid_inspections[guid].append(info_dict)
            if not is_unique:
                error_count += 1
                logger.error(
                    f"Node with {uuid_property_name}={guid} either not exit or not unique.\n{json.dumps(info_dict, indent=2, default=str)}"
                )
            else:
                guid_to_delete.append(guid)
                guids_passed_uniq.append(guid)
    logger.info(f"GUIDs that passed uniqueness check: {len(guids_passed_uniq)}")

    # For guids that passed uniq node test, check if they have upsteam nodes
    logger.info("Checking for upstream nodes for GUIDs that passed uniqueness check.")
    
    if len(guids_passed_uniq) > 5000:
        find_upstream_nodes_results = {}
        logger.warning(f"Large number of GUIDs that passed uniqueness check: {len(guids_passed_uniq)}. The workflow will process them in multiple batches.")
        guids_batches = [guids_passed_uniq[i:i + 5000] for i in range(0, len(guids_passed_uniq), 5000)]
        batch_progress = 0
        for batch in guids_batches:
            batch_progress += 1
            logger.info(f"Processing batch {batch_progress}/{len(guids_batches)}")
            batch_results = myloader.find_upstream_nodes_batch(
                property_name=uuid_property_name,
                property_values=batch
            )
            find_upstream_nodes_results.update(batch_results)
    else:
        find_upstream_nodes_results = myloader.find_upstream_nodes_batch(
            property_name=uuid_property_name,
            property_values=guids_passed_uniq
        )
    logger.info(f"Completed checking for upstream nodes.")
    ## go through find_upstream_nodes_results to check which GUIDs have upstream nodes
    #if_alt_path_upstream_input = []
    #for guid, upstream_nodes in find_upstream_nodes_results.items():
    #    if upstream_nodes:
    #        # extract guid for upstream_nodes only
    #        upstream_nodes_guids = [node["properties"][uuid_property_name] for node in upstream_nodes]
    #        if_alt_path_upstream_input.extend([{"avoid": guid, "target": upstream_node_guid} for upstream_node_guid in upstream_nodes_guids])
    #    else:
    #        pass
    #if if_alt_path_upstream_input: # if the list is not empty
    #    logger.info("Found upstream nodes for the provided guids, will check if alt path to root node can be found for upstream nodes")
    #    if len(if_alt_path_upstream_input) > 5000:
    #        logger.warning("Large number of upstream nodes to check, will process them in multiple batches")
    #        combined_if_alt_path_results = {}
    #        # process it in batches
    #        if_alt_path_upstream_batches = [if_alt_path_upstream_input[i:i + 5000] for i in range(0, len(if_alt_path_upstream_input), 5000)]
    #        batch_progress = 0
    #        for batch in if_alt_path_upstream_batches:
    #            batch_progress += 1
    #            logger.info(f"Processing batch {batch_progress}/{len(if_alt_path_upstream_batches)}")
    #            batch_results = myloader.if_alternative_path_to_root_batch(
    #                property_name=uuid_property_name,
    #                avoid_to_targets_pairs=batch,
    #                root_label=root_node_label  # replace with the actual root label if different
    #            )
    #            # process batch_results as needed
    #            # targets should be {"guid1": True/False}
    #            for avoid, targets in batch_results.items():
    #                if avoid not in combined_if_alt_path_results:
    #                    combined_if_alt_path_results[avoid] = {}
    #                combined_if_alt_path_results[avoid].update(targets)
    #    else:
    #        combined_if_alt_path_results = myloader.if_alternative_path_to_root_batch(
    #            property_name=uuid_property_name,
    #            avoid_to_targets_pairs=if_alt_path_upstream_input,
    #            root_label=root_node_label  # replace with the actual root label if different
    #        )
    #else:
    #    combined_if_alt_path_results = {}

    # go through find_upstream_nodes_results to check if the upstream nodes if they have more than one outgoing edges
    
    if_multi_out_edges = {}
    upstream_node_guids = [node["properties"][uuid_property_name] for upstream_nodes in find_upstream_nodes_results.values() if upstream_nodes for node in upstream_nodes]
    # only look for uniq guids in upstream_node_guids
    upstream_node_guids = list(set(upstream_node_guids))
    if upstream_node_guids:
        logger.info("Checking if upstream nodes have multiple outgoing edges.")
        if len(upstream_node_guids) > 5000:
            batch_progress = 0
            upstream_node_guid_batches = [upstream_node_guids[i:i + 5000] for i in range(0, len(upstream_node_guids), 5000)]
            for batch in upstream_node_guid_batches:
                batch_progress += 1
                logger.info(f"Processing batch {batch_progress}/{len(upstream_node_guid_batches)}")
                if_multi_out_edges.update(myloader.if_multiple_outgoing_edges_batch(
                    property_name=uuid_property_name,
                    property_values=batch
                ))
        else:
            if_multi_out_edges.update(myloader.if_multiple_outgoing_edges_batch(
                property_name=uuid_property_name,
                property_values=upstream_node_guids
            ))
        logger.info("Completed checking if upstream nodes have multiple outgoing edges.")
    else:
        logger.info("No upstream nodes found with the provided GUIDs")
        pass # upstream_node_guids is empty, no need to look for outgoig edges of these upstream nodes

    logger.info("Preparing for GUIDs inspection results")
    for guid, upstream_nodes in find_upstream_nodes_results.items():
        if upstream_nodes: # this guid is not a leaf node, so upsteam_nodes is not empty
            unfound_upstream_nodes = []
            for upstream_node in upstream_nodes: # inspect every upstream node
                upstream_node_guid = upstream_node["properties"][uuid_property_name]
                if upstream_node_guid not in guid_list:
                    if_upstream_node_alt = if_multi_out_edges[upstream_node_guid] # check if the upstream node has multiple outgoing edges
                    if if_upstream_node_alt:
                        upstream_node = {
                            "warning": f"This upstream/child node has multiple outgoing edges, which means it may have alternative paths to a root node. Delete with caution.",
                            **upstream_node,
                        }
                    unfound_upstream_nodes.append(upstream_node)
                    guid_to_delete.append(upstream_node_guid)
                else:
                    # upstream_node_guid already in guid_list, when testing unique of this guid, it will be added to the guid_to_delete
                    pass
            if not unfound_upstream_nodes:
                    guid_inspections[guid].append({
                        "check_item": "upstream/children node check",
                        "check_result": "Pass",
                        "message": f"This is NOT a leaf node, but All upstream/children nodes (if any) can be found in the provided guid list",
                        "unfound_upstream_node(s)": unfound_upstream_nodes # this should be an empty list
                    })
                    logger.info(f"Node with {uuid_property_name}={guid} is NOT a leaf node. But all upstream/children nodes (if any) can be found in the provided guid list.")
            else:
                guid_inspections[guid].append({
                    "check_item": "upstream/children node check",
                    "check_result": "Fail",
                    "message": f"This is NOT a leaf node. Found upstream/children nodes that are not included in the provided guid list, which may cause orphan node issue if deleted. Please review the unfound upstream nodes for this guid.",
                    "unfound_upstream_node(s)": unfound_upstream_nodes
                })
                logger.error(f"Node with {uuid_property_name}={guid} is NOT a leaf node. Found upstream/children nodes that are not included in the provided guid list. Please review the unfound upstream nodes for this guid.")
                error_count += 1 # increment error if there are any upstream/children nodes of the target node that are not included in the original guid list.
        else: # no upstream nodes found, this is a leaf node
            guid_inspections[guid].append({
                "check_item": "upstream/children node check",
                "check_result": "Pass",
                "message": f"This is a leaf node. No upstream/children nodes found.",
                "unfound_upstream_node(s)": [] # this should be an empty list
            })
            logger.info(f"Node with {uuid_property_name}={guid} is a leaf node. No upstream/children nodes found.")

    # parse output bucket location
    output_bucket, output_folder = parse_file_url(output_bucket_loc)
    output_subfolder = f"precision_deletion_{get_time()}"

    # write guid_to_delete into a file
    
    if len(guid_to_delete) > 0:
        guid_to_delete_output_file = f"guid_ready_to_delete_{get_time()}.json"
        with open(guid_to_delete_output_file, "w", encoding="utf-8") as f:
            json.dump(guid_to_delete, f, indent=2, default=str)
        file_ul(bucket=output_bucket, output_folder=output_folder, sub_folder=output_subfolder, newfile=guid_to_delete_output_file)
        logger.info(f"guid ready to delete list has been uploaded to {output_subfolder} under bucket {output_bucket_loc} for review")
        logger.warning("guid ready to delete list DOES NOT contain any guid that does not exist or not unique in the database. The list ONLY contains guids that passed uniquness test (from the provided guid list) and any potential upstream/children nodes of the provided guids")
    else:
        logger.warning(
            "guid ready to delete list is empty. No guid passed the inspection, no deletion can be performed."
        )

    # write guid inspection results into a file
    # upload guid inspection results to s3 for review
    inspection_output_file = f"guid_inspection_{get_time()}.json"
    with open(inspection_output_file, "w", encoding="utf-8") as f:
        json.dump(guid_inspections, f, indent=2, default=str)
    file_ul(bucket=output_bucket, output_folder=output_folder, sub_folder=output_subfolder, newfile=inspection_output_file)
    logger.info(f"inspection results have been uploaded to {output_subfolder} under bucket {output_bucket_loc} for review")

    if error_count > 0: # issues were found in the guid inspection, no deletion will be performed even if dry_run is set to false
        logger.warning("We found potential issues with the guid(s) to be deleted. Please review the inspection results for details.")
    else: # no issue found with provided guid. It is safe to proceed with deletion if dry run is not enabled.
        logger.info("No potential issue found with the guid(s) to be deleted based on our checks.")
        if dry_run:
            logger.info(f"Dry run enabled. No data nodes will be deleted.")
            logger.info(
                f"guid ready to delete list has been uploaded to {output_subfolder} under bucket {output_bucket_loc} for review"
            )
            logger.warning(
                "guid ready to delete list DOES NOT contain any guid that does not exist or not unique in the database. The list ONLY contains guids that passed uniquness test (from the provided guid list) and any potential upstream/children nodes of the provided guids"
            )
        else:
            logger.info(f"Dry run disabled. The nodes provided ({len(guid_to_delete)}) will be deleted")
            total_deleted = myloader.delete_nodes_by_prop_value(identifier_list=guid_to_delete, property_name=uuid_property_name)
            logger.info(f"Total nodes deleted: {total_deleted}")
            logger.info(
                f"Deleted guid list ({guid_to_delete_output_file}) has been uploaded to {output_subfolder} under bucket {output_bucket_loc} for review"
            )

            # enforce a finding orphan nodes check after deletion to make sure there is no orphan node left in the database
            logger.info("Performing a check for orphan nodes after deletion.")
            floating_nodes_filename = (
                f"nodes_no_path_to_{root_node_label}_{get_time()}.json"
            )
            find_floating_db_nodes_flow(
                loader=myloader,
                output_filename=floating_nodes_filename,
                root_node_label=root_node_label,
            )
            floating_nodes_length = find_json_length(floating_nodes_filename)
            if floating_nodes_length == 0:
                logger.info("No floating nodes found in the database after deletion.")
            else:
                logger.error(
                    f"Found {floating_nodes_length} floating nodes (without a path to the root node) in the database."
                )
                file_ul(
                    bucket=output_bucket,
                    output_folder=output_folder,
                    sub_folder=output_subfolder,
                    newfile=floating_nodes_filename,
                )
                logger.info(f"Uploaded the orphan nodes report ({floating_nodes_filename}) to {output_subfolder} under bucket {output_bucket_loc} for review.")

    logger.info("Precision deletion flow completed.")
