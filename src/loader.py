from time import time
import pandas as pd
from typing import Dict, Generator, Any
from operator import itemgetter
from itertools import groupby
import os
from neo4j.exceptions import ClientError, ServiceUnavailable, TransientError
from neo4j_viz.neo4j import from_neo4j
from neo4j.graph import Path
from timeit import default_timer as timer
import json


class Loader:
    def __init__(self, driver: "GraphDatabase.driver"):
        self.driver = driver

    def close(self):
        self.driver.close()

    @staticmethod
    def chunks(lst: list, size:int) -> Generator[list, None, None]:
        """A generator which yields an input list by a given size

        Args:
            lst (list): input list to split
            size (int): chunk size

        Yields:
            list: chunks of the input list
        """
        for i in range(0, len(lst), size):
            yield lst[i : i + size]

    @staticmethod
    def check_encoding(file_path: str) -> str:
        """Check encoding of a file, either utf8 or windows1252
        borrowed from: https://github.com/CBIIT/icdc-dataloader/blob/3.2.0_memgraph_loader/data_loader.py#L116

        Args:
            file_path (str): file path to check

        Returns:
            str: encoding type
        """
        utf8 = "utf-8"
        cp1252 = "cp1252"
        try:
            with open(file_path, encoding=utf8) as file:
                for _ in file.readlines():
                    pass
            return utf8
        except UnicodeDecodeError:
            return cp1252

    @staticmethod
    def read_file_in_chunks(
        file_path: str, encoding: str = "utf-8", chunk_size: int = 3000
    ) -> Generator[pd.DataFrame, None, None]:
        """

        Args:
            file_path (str): tsv file path
            encoding (str): file encoding type, utf-8 or cp1252. Default to "utf-8"
            chunk_size (int, optional): number of rows per chunk. Defaults to 1000.

        Yields:
            Iterator[pd.DataFrame]: DataFrame chunks containing 'chunk_size' rows each
        """
        try:
            reader = pd.read_csv(
                file_path,
                sep="\t",
                #dtype=str, # let pandas to infer data types
                encoding=encoding,
                chunksize=chunk_size,
                quotechar='"',
                doublequote=True,
                escapechar="\\", # add escape char to handle special characters
                keep_default_na=False,
                na_values=[""],  # treat empty strings as NaN
            )
            for chunk in reader:
                # which row contains data node properties as well as relationships
                yield chunk
        except Exception as e:
            print(f"Error reading {file_path}: {e}")
            raise e

    @staticmethod
    def generate_chunk_records(
        chunk: pd.DataFrame, model_parser: "ModelParser",subgraph_col: str|None = None
    ) -> tuple[str, list[dict]]:
        """
        Generate records from a given chunk of data based on the model parser.

        Args:
            chunk (pd.DataFrame): DataFrame chunk containing data rows
            subgraph_col (str|None): The subgraph column name. This column indicates which subgraph the data entries belong to. In most cases, the value under this subgraph column is a study id/accession. Defaults to None.
            id_field (str, optional): The unique identifier field. Defaults to "guid".
        Returns:
            tuple[str, list[dict]]: A tuple containing (chunk_type, list of records converted from the chunk)
        """
        chunk_type = chunk["type"].iloc[
            0
        ]  # assuming all rows in the chunk are of the same type. If not, the tsv shouldn't pass validation

        columns = chunk.columns.tolist()
        # remove "type" column, subgraph column, and relationship columns
        columns_to_keep = [
            col
            for col in columns
            if col != subgraph_col and col != "type" and "." not in col
        ]
        chunk_filtered = chunk[columns_to_keep]
        # create a list of records
        records = []
        for record in chunk_filtered.to_dict(orient="records"):
            # check if there is any missing value in the properties, collect these keys and remove them
            keys_to_remove = []
            for key in record:
                # find keys to empty str which interpreted as NaN by pandas
                if pd.isna(record[key]):
                    keys_to_remove.append(key)
                else:
                    # let's not convert to string to preserve data types
                    pass
            cleaned_record = {k: v for k, v in record.items() if k not in keys_to_remove}
            # for remaining keys, if the cleaned_record value is an int/floar, check the model
            # we have cases of str type property that are mis inferred as number/int during loading
            for u in cleaned_record:
                if isinstance(cleaned_record[u], (int, float)):
                    expected_type = model_parser.get_prop_type(node_name=chunk_type, prop_name=u)
                    if expected_type == "number" or expected_type == "integer":
                        # all good
                        pass
                    else:
                        # convert to str
                        cleaned_record[u] = str(cleaned_record[u])
                else:
                    # all good
                    pass
            records.append(cleaned_record)
        return chunk_type, records

    # upsert records of a chunk with session.begin_transaction as input
    @staticmethod
    def upsert_chunk_records_with_tx(
        tx, node_type: str, records: list[dict], id_field: str = "guid"
    ):
        """
        Upsert a list of records into a graph database using the provided transaction.
        Use this method when you want to participate in a larger transaction context.

        Args:
            tx: The Neo4j transaction to use.
            node_type (str): The type/label of the nodes to be upserted.
            records (list[dict]): A list of records to be upserted.
            id_field (str, optional): The unique identifier field. Defaults to "guid".
        """
        cypher_statement = f"""
        UNWIND $records AS record
        MERGE (n:{node_type} {{ {id_field}: record.{id_field} }})
        ON CREATE SET n += record, n.created = dateTime()
        ON MATCH SET n += record, n.updated = dateTime()
        """
        with tx.begin_transaction() as ts:
            try:
                results = ts.run(cypher_statement, records=records)
                summary = results.consume()
                return vars(summary.counters)
            except Exception as e:
                print("Error upserting records: ", e)
                ts.rollback()
                raise e

    # upsert all records of a file
    def upsert_file_records(
        self,
        file_path: str,
        model_parser: "ModelParser",
        subgraph_col: str | None = None,
        id_field: str = "guid",
        chunk_size: int = 3000,
    ) -> dict:
        """
        Upsert records from a TSV file into the graph database in chunks.

        Args:
            file_path (str): The path to the TSV file.
            subgraph_col (str|None): The subgraph column name. Defaults to None.
            id_field (str, optional): The unique identifier field. Defaults to "guid".
            chunk_size (int, optional): Number of rows per chunk. Defaults to 3000.

        Returns:
            dict: Summary counters from all chunks processed.
        """
        encoding = self.check_encoding(file_path)
        summary_list = []

        # Use a single session for all chunks
        batch_count = 0
        print(f"Start processing file {os.path.basename(file_path)}")
        with self.driver.session() as tx:
            # with session.begin_transaction() as tx:
            for chunk in self.read_file_in_chunks(file_path, encoding, chunk_size):
                batch_count += 1
                print(f"Processing batch {batch_count}...")
                batch_begin = timer()
                chunk_type, records = self.generate_chunk_records(chunk=chunk, model_parser=model_parser,subgraph_col=subgraph_col)
                result_summary = self.upsert_chunk_records_with_tx(
                    tx, chunk_type, records, id_field
                )
                batch_end = timer()
                print(
                    f"Batch {batch_count} created {result_summary['nodes_created']} nodes"
                )
                print(
                    f"Batch {batch_count} set {result_summary['properties_set']} properties"
                )
                print("Batch loading time (seconds): ", batch_end - batch_begin)
                summary_list.append(result_summary)

        # combine counts in all summaries into one
        return_summary = {key: 0 for key in summary_list[0].keys()}
        for summary in summary_list:
            for key, value in summary.items():
                return_summary[key] += value
        return return_summary

    @staticmethod
    def generate_chunk_relationships(
        chunk: pd.DataFrame, model_parser: "ModelParser", id_field: str = "guid", delimiter: str = ";"
    ) -> list[dict]:
        """
        Generate a list of relationship records from a chunk of data.

        Args:
            chunk (pd.DataFrame): The chunk of data to process.
            model_parser (ModelParser): The model parser instance to use.
            id_field (str, optional): The unique identifier field. Defaults to "guid".
            delimiter (str, optional): The delimiter used in the case of one to many relationship. Defaults to ";".

        Returns:
            list[dict]: A list of relationship records.
        """
        # type column must present. Otherwise the file won't pass validation
        chunk_type = chunk["type"].iloc[0]
        columns = chunk.columns.tolist()
        # filter out edge columns, such as <parent_node>.guid
        edge_columns = [col for col in columns if "." in col]
        # only need guid, and edge columns
        edges_to_add = []
        # edge is usually <parent_node>.<prop>
        for edge in edge_columns:
            edge_parent, edge_parent_prop = edge.split(".")
            # this is the edge handle/label
            edge_handle = model_parser.get_edge_handle(
                edge_src=chunk_type, edge_dst=edge_parent
            )
            # remove any row if edge is NaN
            # empty str is now recognized as NaN by pandas
            chunk_filtered = chunk[~chunk[edge].isna()]
            # extract only two columns, id_field and edge
            chunk_filtered = chunk_filtered[[id_field, edge]]
            # if there is edge left to establish
            if chunk_filtered.shape[0] > 0:
                edges_list = chunk_filtered.to_dict(orient="records")
                for item in edges_list:
                    if delimiter not in item[edge]:
                        edge_item = {}
                        edge_item["src_label"] = chunk_type
                        edge_item["src_prop"] = id_field
                        edge_item["src_match"] = item[id_field]
                        edge_item["dst_label"] = edge_parent
                        edge_item["dst_prop"] = edge_parent_prop
                        edge_item["dst_match"] = item[edge]
                        edge_item["handle"] = edge_handle
                        edges_to_add.append(edge_item)
                    else:
                        # one to many relationship
                        dst_matches = item[edge].split(delimiter)
                        for dst in dst_matches:
                            edge_item = {}
                            edge_item["src_label"] = chunk_type
                            edge_item["src_prop"] = id_field
                            edge_item["src_match"] = item[id_field]
                            edge_item["dst_label"] = edge_parent
                            edge_item["dst_prop"] = edge_parent_prop
                            edge_item["dst_match"] = dst.strip()
                            edge_item["handle"] = edge_handle
                            edges_to_add.append(edge_item)
            else:
                # there is no edge to establish with this parent node
                pass
        return edges_to_add

    @staticmethod
    def upsert_chunk_relationships_with_tx(tx, edge_list: list[dict]) -> dict:
        """Upsert relationships in the database with a list of dictionaries that specify the edges.
        A edge item example would be:
        {
            "src_label": "sample",
            "src_prop": "guid",
            "src_match": "123",
            "dst_label": "participant",
            "dst_prop": "guid",
            "dst_match": "456",
            "handle": "of_sample"
        }

        Args:
            tx (session.begin_transaction): A neo4j transaction object.
            edge_list (list[dict]): A list of edge dictionaries to upsert.
        """
        # we sorted the edge_list and make groups based off src_label, dst_label, and handle
        edges_sorted = sorted(
            edge_list, key=itemgetter("src_label", "dst_label", "handle")
        )
        grouped_edges = {
            key: list(group)
            for key, group in groupby(
                edges_sorted, key=itemgetter("src_label", "dst_label", "handle")
            )
        }
        summary_list = []
        with tx.begin_transaction() as ts:
            for (src_label, dst_label, handle), group in grouped_edges.items():
                # create variable for src_prop, and dst_prop
                src_prop = group[0]["src_prop"]
                dst_prop = group[0]["dst_prop"]

                # cypehr statement to upsert relationships in this group
                cypher = f"""
                UNWIND $edges AS edge
                MATCH (src:{src_label} {{{src_prop}: edge.src_match}})
                MATCH (dst:{dst_label} {{{dst_prop}: edge.dst_match}})
                MERGE (src)-[r:{handle}]->(dst)
                ON CREATE SET r.created = datetime()
                ON MATCH SET r.updated = datetime()
                """
                params = {"edges": group}
                try:
                    results = ts.run(cypher, **params)
                    summary = results.consume()
                    print(
                        f"Relationships created for {src_label}-{handle}->{dst_label}:",
                        summary.counters.relationships_created,
                    )
                    summary_list.append(vars(summary.counters))
                except Exception as e:
                    print("Error upserting records: ", e)
                    ts.rollback()
                    raise e
        # combine counts in all summaries into one
        return_summary = {key: 0 for key in summary_list[0].keys()}
        for summary in summary_list:
            for key, value in summary.items():
                return_summary[key] += value
        return return_summary

    def upsert_file_relationships(
        self,
        file_path: str,
        model_parser: "ModelParser",
        id_field: str = "guid",
        chunk_size: int = 3000,
        delimiter: str = ";"
    ) -> dict:
        """Upsert relationships of a given file
        Relationships can only be done when both parent and child nodes have been created

        Args:
            file_path (str): The path to the file to process.
            id_field (str, optional): The name of the ID field. Defaults to "guid".
            chunk_size (int, optional): The size of the chunks to process. Defaults to 3000.

        Returns:
            dict: A summary of the upsert operation.
        """
        encoding = self.check_encoding(file_path)
        summary_list = []
        batch_count = 0

        # Use a single session but separate transactions for each chunk
        print(f"Start processing file {os.path.basename(file_path)}")
        with self.driver.session() as tx:
            for chunk in self.read_file_in_chunks(file_path, encoding, chunk_size):
                batch_count += 1
                print(f"Processing batch {batch_count}...")

                chunk_relationships = self.generate_chunk_relationships(
                    chunk=chunk, id_field=id_field, model_parser=model_parser, delimiter=delimiter
                )
                # for study/root node tsv, there shouldn't be any edges.
                if len(chunk_relationships) > 0:
                    # Each chunk gets its own transaction with retry logic
                    max_retries = 3
                    retry_count = 0

                    while retry_count < max_retries:
                        try:
                            # with session.begin_transaction() as tx:
                            summary = self.upsert_chunk_relationships_with_tx(
                                tx, edge_list=chunk_relationships
                            )
                            summary_list.append(summary)
                            print(
                                f"Batch {batch_count} completed: {summary.get('relationships_created', 0)} relationships created"
                            )
                            break  # Success, exit retry loop
                        except Exception as e:
                            retry_count += 1
                            print(
                                f"Batch {batch_count} failed (attempt {retry_count}/{max_retries}): {e}"
                            )
                            if retry_count >= max_retries:
                                print(
                                    f"Batch {batch_count} failed after {max_retries} attempts, skipping..."
                                )
                                # Optionally re-raise the exception or continue
                                raise e
                            else:
                                print(f"Retrying batch {batch_count}...")
                else:
                    print(f"Batch {batch_count} skipped: no relationships to create")

        # if summary_list is empty
        if len(summary_list) == 0:
            return {
                "labels_added": 0,
                "labels_removed": 0,
                "nodes_created": 0,
                "nodes_deleted": 0,
                "properties_set": 0,
                "relationships_created": 0,
                "relationships_deleted": 0,
            }
        else:
            # combine counts in all summaries into one
            return_summary = {key: 0 for key in summary_list[0].keys()}
            for summary in summary_list:
                for key, value in summary.items():
                    return_summary[key] += value
            return return_summary

    def _list_index(self) -> list:
        """List all indexes in the database.
        Example of returned index:
        [{'label': 'cell_line', 'property': 'id'}, {'label': 'clinical_measure_file', 'property': 'id'}]
        """
        primary_query = "SHOW INDEXES;"
        fallback_query = "SHOW INDEX INFO;"
        with self.driver.session() as session:
            try:
                result = session.run(primary_query)
            except ClientError as e:
                print("Primary index query failed, trying fallback query...")
                result = session.run(fallback_query)
            indexes = []
            for record in result:
                if record.get("index type") == "label+property":
                    indexes.append(
                        {
                            "label": record.get("label"),
                            "property": record.get("property")[0],
                        }
                    )
                else:
                    pass
        return indexes

    def create_index(
        self, model_parser: "ModelParser", id_field: str = "guid"
    ) -> list[dict]:
        """Create indexes based on the model parser definitions.
        Returns a list of created indexes.
        """
        created_indexes = []
        existing_indexes = self._list_index()
        model_node_list = model_parser.get_node_list()
        with self.driver.session() as session:
            for node in model_node_list:
                exist = False
                for idx in existing_indexes:
                    if idx["label"] == node and idx["property"] == id_field:
                        # index already exist
                        created_indexes.append({"label": node, "property": id_field})
                        exist = True
                    else:
                        pass
                if not exist:
                    # create index
                    query = f"CREATE INDEX ON :{node}({id_field});"
                    try:
                        session.run(query)
                        print(f"Index created: {node}({id_field})")
                        created_indexes.append({"label": node, "property": id_field})
                    except Exception as e:
                        print(f"Error creating index for {node}({id_field}): ", e)
                        raise e
        return created_indexes

    def drop_index(self, index_list: list[dict]) -> None:
        """Drop indexes based on the provided list of indexes.
        Each index in the list should be a dict with 'label' and 'property' keys.
        """
        with self.driver.session() as session:
            for index in index_list:
                label = index["label"]
                property_name = index["property"]
                query = f"DROP INDEX ON :{label}({property_name});"
                try:
                    session.run(query)
                    print(f"Index dropped: {label}({property_name})")
                except Exception as e:
                    print(f"Error dropping index for {label}({property_name}): ", e)
                    raise e
        return None

    def drop_all_indexes(self) -> None:
        """Drop all indexes in the database."""
        existing_indexes = self._list_index()
        print(existing_indexes)
        self.drop_index(existing_indexes)
        return None

    def wipe_database(self, batch_size: int = 10000) -> int:
        """Wipe the entire database.
        Delete in batches
        """
        delete_nodes = 0
        with self.driver.session() as session:
            with session.begin_transaction() as tx:
                while True:
                    cypher = """
                    MATCH (n)
                    WITH n LIMIT $batch_size
                    DETACH DELETE n
                    RETURN count(n) AS deleted_count
                    """
                    try:
                        result = tx.run(cypher, batch_size=batch_size)
                        deleted_count = result.single()["deleted_count"]
                        delete_nodes += deleted_count
                        if deleted_count == 0:
                            break
                    except Exception as e:
                        print("Error wiping database: ", e)
                        tx.rollback()
                        raise e
        return delete_nodes

    def wipe_rooted_subgraph(
        self, root_node_label: str, root_node_prop: str, subgraph_value: str, chunk_threshold: int = 10000
    ) -> dict:
        """Wipe a rooted subgraph by deleting all nodes and relationships connected within.
        Args:
            root_node_label (str): root node label, such as study
            root_node_prop (str): root node property for matching, such as study_id, dbgap_accession
            subgraph_value (str): the value of root node property to identify the subgraph, such as a study accession phs002790.
            chunk_threshold (int): threshold to determine if the subgraph is large and needs to be deleted in batches. Default to 10,000 nodes.
            
        Returns:
            dict: summary of deleted nodes and relationships
        """

        node_ids = self._get_rooted_subgraph_nodes_ids(
            root_node_label, root_node_prop, subgraph_value
        )

        deleted_nodes = 0
        detached_rels = 0
        if len(node_ids) == 0:
            print(f"No nodes found in root node {root_node_label} with {root_node_prop}={subgraph_value}. Nothing to delete.")
        # found nodes belonging to the subgraph
        else:
            print(f"{len(node_ids)} nodes found in root node {root_node_label} with {root_node_prop}={subgraph_value}. Start deleting...")
            if len(node_ids) > chunk_threshold:
                # large subgraph to delete, print warning and delete in batches
                print(f"Warning: large subgraph detected with {len(node_ids)} nodes, deletion may take a while...")
                # first detach relationships in batches
                print("Start detaching relationships...")
                rel_batch_count = 0
                for batch in self.chunks(node_ids, 5000): # chunk it for 5000 by default
                    rel_batch_count += 1
                    detached_nodes_count = self._detach_nodes_by_ids(node_ids=batch)
                    print(f"Detached rels count for batch {rel_batch_count}: ", detached_nodes_count)
                    detached_rels += detached_nodes_count
                print(f"Total relationships detached: {detached_rels}")
                # then delete nodes in batches
                print("Start deleting nodes...")
                node_batch_count = 0
                for batch in self.chunks(node_ids, 5000): # chunk it for 5000 by default
                    node_batch_count += 1
                    deleted_nodes_count = self._delete_nodes_by_ids(node_ids=batch)
                    print(f"Deleted nodes count for batch {node_batch_count}: ", deleted_nodes_count)
                    deleted_nodes += deleted_nodes_count
                print(f"Total nodes deleted: {deleted_nodes}")
            else:
                # small subgraph, no batching needed
                print("Start deleting nodes and relationships...")
                # first detach relationships
                detached_rels = self._detach_nodes_by_ids(node_ids=node_ids)
                print(f"Total relationships detached: {detached_rels}")
                # then delete nodes
                deleted_nodes = self._delete_nodes_by_ids(node_ids=node_ids)
                print(f"Total nodes deleted: {deleted_nodes}")
        return {"nodes_deleted": deleted_nodes, "relationships_detached": detached_rels}

    def _get_rooted_subgraph_nodes_ids(self, root_node_label: str, root_node_property: str, subgraph_value: str) -> list[int]:
        """Returns a list of node id property values in a rooted subgraph defined by the root node, such as study node.

        Args:
            root_node_label (str): root node label, such as study
            root_node_property (str): root node property for matching, such as study_id, dbgap_accession
            subgraph_value (str): the value of root node property to identify the subgraph

        Returns:
            list[int]: a list of IDs of all nodes in the subgraph
        """
        query = f"""
        MATCH (s:{root_node_label} {{{root_node_property}: $subgraph_value}})
        OPTIONAL MATCH (s)<-[*]-(n)
        WITH COLLECT(DISTINCT id(s)) + COLLECT(DISTINCT id(n)) AS node_ids
        RETURN node_ids
        """
        with self.driver.session() as session:
            result = session.run(query, subgraph_value=subgraph_value)
            record = result.single()
            return record["node_ids"] if record else []

    def _detach_nodes_by_ids(self, node_ids: list[int]) -> int:
        """detach any relationship of matched nodes using a list of IDs 

        Args:
            node_ids (list[int]): A list of IDs of nodes

        Returns:
            int: number of deleted rekationships
        """
        query = """
        UNWIND $node_ids AS nid
        MATCH (n)
        WHERE id(n) = nid
        MATCH (n)-[r]-()
        WITH DISTINCT r
        DELETE r
        RETURN count(r) AS deleted_rels
        """
        with self.driver.session() as session:
            result = session.run(query, node_ids=node_ids)
            record = result.single()
            return record["deleted_rels"] if record else 0

    def _delete_nodes_by_ids(self, node_ids: list[int]) -> int:
        """delete nodes using a list of IDs AFTER detaching relationships

        Args:
            node_ids (list[int]): A list of IDs of nodes

        Returns:
            int: number of deleted nodes
        """
        query = """
        UNWIND $node_ids AS nid
        MATCH (n)
        WHERE id(n) = nid
        WITH DISTINCT n
        DELETE n
        RETURN count(n) AS deleted_nodes
        """
        with self.driver.session() as session:
            result = session.run(query, node_ids=node_ids)
            record = result.single()
            return record["deleted_nodes"] if record else 0

    def viz_intermediate_anchored_traversals(
        self,
        root_node_label: str,
        root_node_prop: str,
        root_node_prop_value: str,
        intermediate_root_node_label: str,
        intermediate_root_node_prop: str,
        intermediate_root_node_prop_value: str,
        viz_filename: str|None = "intermediate_anchored_traversals.html",
    ) -> str:
        """Identify all possible descendant traversals from an intermedaite root node which sits in within a larger rooted subgraph.
        A common use case is to identify all traversals from a participant node (intermediate root node) within a study subgraph (root node). For example we havean ineligible participant that we want to delete from the a study subgraph, we first need to identify all the descendant traversals from this participant node that we want to delete.

        Args:
            root_node_label (str): The label of the first root node.
            root_node_prop (str): The property name of the first root node.
            root_node_prop_value (str): The property value of the first root node.
            intermediate_root_node_label (str): The label of the intermediate root node.
            intermediate_root_node_prop (str): The property name of the intermediate root node.
            intermediate_root_node_prop_value (str): The property value of the intermediate root node.
            viz_filename (str): The output html filename for the visualization, such as path.html

        Returns:
            str | None: file names of the visualization html and json data
        """
        query = f"""
        MATCH p=(r:{root_node_label} {{{root_node_prop}: $root_node_prop_value}})<-[*1..7]-(i:{intermediate_root_node_label} {{{intermediate_root_node_prop}: $intermediate_root_node_prop_value}})<-[*0..7]-(n)
        RETURN p
        """
        with self.driver.session() as session:
            graph_obj = session.run(
                query,
                root_node_prop_value=root_node_prop_value,
                intermediate_root_node_prop_value=intermediate_root_node_prop_value,
            ).graph()

        if len(graph_obj.nodes) == 0:
            print("No graph data found for the given intermediate anchored traversal.")
            return None
        if len(graph_obj.nodes) > 1000:
            print("Warning: large graph deteted with more than 1000 nodes, graph too large to render safely.")
            return None
        else:
            VG = from_neo4j(graph_obj)
            html_obj = VG.render(layout="forcedirected",height="900px")
            html_str = getattr(html_obj, "data", None)
            if html_str is None:
                html_str = str(html_obj)
            with open(viz_filename, "w", encoding="utf-8") as f:
                f.write(html_str)
            return viz_filename

    def export_intermediate_anchored_traversals(
        self,
        root_node_label: str,
        root_node_prop: str,
        root_node_prop_value: str,
        intermediate_root_node_label: str,
        intermediate_root_node_prop: str,
        intermediate_root_node_prop_value: str,
        model_parser: "ModelParser",
        output_filename: str = "intermediate_anchored_traversals.json",
        max_retries: int = 3,
        base_sleep_time: float = 1.0,
        id_field: str = "guid",
    ) -> str:
        """Identify all possible descendant traversals from an intermedaite root node which sits in within a larger rooted subgraph.
        A common use case is to identify all traversals from a participant node (intermediate root node) within a study subgraph (root node). For example we havean ineligible participant that we want to delete from the a study subgraph, we first need to identify all the descendant traversals from this participant node that we want to delete.

        Args:
            root_node_label (str): The label of the first root node.
            root_node_prop (str): The property name of the first root node.
            root_node_prop_value (str): The property value of the first root node.
            intermediate_root_node_label (str): The label of the intermediate root node.
            intermediate_root_node_prop (str): The property name of the intermediate root node.
            intermediate_root_node_prop_value (str): The property value of the intermediate root node.
            output_filename (str | None): The output filename for the JSON data, such as path.json
            model_parser: "ModelParser",
            max_retries (int): Maximum number of retries for transient errors.
            base_sleep_time (float): Base sleep time in seconds for exponential backoff.
            id_field (str, optional): The unique identifier field for each node. Defaults to "guid".

        Returns:
            str: file names of the visualization html and json data
        """
        query = f"""
        MATCH p=(r:{root_node_label} {{{root_node_prop}: $root_node_prop_value}})<-[*1..7]-(i:{intermediate_root_node_label} {{{intermediate_root_node_prop}: $intermediate_root_node_prop_value}})<-[*0..7]-(n)
        RETURN p
        """
        return_path = []

        for attempt in range(1, max_retries + 1):
            try:
                with self.driver.session() as session:
                    rows = session.run(
                        query,
                        root_node_prop_value=root_node_prop_value,
                        intermediate_root_node_prop_value=intermediate_root_node_prop_value,
                    )
                    for record in rows:
                        path_obj = record["p"]
                        if not path_obj:
                            continue

                        compact_dict = self.cypher_path_to_compact_dict(
                            p=path_obj,
                            model_parser=model_parser,
                            id_field=id_field,
                        )
                        return_path.append(compact_dict)
                break  # Exit the retry loop if successful
            except (ServiceUnavailable, TransientError) as e:
                if attempt == max_retries:
                    raise e
                sleep_time = base_sleep_time * (2 ** (attempt - 1))
                time.sleep(sleep_time)
        # write to output file
        with open(output_filename, "w", encoding="utf-8") as f:
            json.dump(return_path, f, indent=4)
        return output_filename

    @staticmethod
    def cypher_path_to_compact_dict(p:Path, model_parser: "ModelParser", id_field: str = "guid") -> Dict[str, Any]:
        """Extracts selected fields from Path object and returns a compact dict

        Args:
            p (Path): A neo4j.graph.Path object from neo4j cypger query with path reutrn
            model_parser (ModelParser): A model parser instance
            id_field (str, optional): The unique identifier field. Defaults to "guid". 

        Returns:
            Dict[str, Any]: a compact dictionary representing the path with two keys, "nodes" and "relationships"
        """
        path_dict = {"nodes": [], "relationships": []}
        # process nodes
        for node in p.nodes:
            labels = list(node.labels)
            node_type = labels[0] if labels else None
            node_key_prop = model_parser.get_node_key_prop(node_name=node_type)

            node_dict = {
                            "ID": getattr(node, "element_id", None) or getattr(node, "id", None),
                            "type": node_type,
                            "properties": {},
                        }
            if node_key_prop:
                node_dict["properties"][node_key_prop] = node.get(node_key_prop)
            node_dict["properties"][id_field] = node.get(id_field)
            path_dict["nodes"].append(node_dict)
        # process relationships
        for rel in p.relationships:
            rel_dict = {
                            "ID": getattr(rel, "element_id", None) or getattr(rel, "id", None),
                            "type": rel.type,
                            "start_node": {
                                "ID": getattr(rel.start_node, "element_id", None)
                                      or getattr(rel.start_node, "id", None),
                                "type": next(iter(rel.start_node.labels), None),
                            },
                            "end_node": {
                                "ID": getattr(rel.end_node, "element_id", None)
                                      or getattr(rel.end_node, "id", None),
                                "type": next(iter(rel.end_node.labels), None),
                            },
                        }
            path_dict["relationships"].append(rel_dict)
        return path_dict

    def find_intermediate_anchored_descendants(
        self,
        root_node_label: str,
        root_node_prop: str,
        root_node_prop_value: str,
        intermediate_root_node_label: str,
        intermediate_root_node_prop: str,
        intermediate_root_node_prop_value: str,
        model_parser: "ModelParser",
        id_field: str = "guid",
    ) -> list[dict[str, Any]]:
        """Identify all possible descendant traversals from an intermedaite root node which sits within a larger rooted subgraph.
        A common use case is to identify all traversals from a participant node (intermediate root node) within a study subgraph (root node). For example we havean ineligible participant that we want to delete from the a study subgraph, we first need to identify all the descendant traversals from this participant node that we want to delete.
        This function returns a list of node IDs (descendants of intermediate_root_node and intermediate root node ITSELF) in the traversals

        Args:
            root_node_label (str): The label of the first root node.
            root_node_prop (str): The property name of the first root node.
            root_node_prop_value (str): The property value of the first root node.
            intermediate_root_node_label (str): The label of the intermediate root node.
            intermediate_root_node_prop (str): The property name of the intermediate root node.
            intermediate_root_node_prop_value (str): The property value of the intermediate root node.
            model_parser: "ModelParser",
            id_field (str, optional): The unique identifier field for each node. Defaults to "guid".

        Returns:
            list[dict[str, Any]]: A list of dictionaries representing the traversals.
        """
        # the relationship distance is set to 0-7, between the intermediate node and its descendants, so the return will include the intermediate root node itself
        query = f"""
        MATCH (r:{root_node_label} {{{root_node_prop}: $root_node_prop_value}})<-[*1..7]-(i:{intermediate_root_node_label} {{{intermediate_root_node_prop}: $intermediate_root_node_prop_value}})<-[*1..7]-(n)
        WITH  [i] + collect(DISTINCT n) as nodes
        RETURN nodes
        """
        with self.driver.session() as session:
            result = session.run(
                query,
                root_node_prop_value=root_node_prop_value,
                intermediate_root_node_prop_value=intermediate_root_node_prop_value,
            )
            record = result.single()
            return_nodes = record["nodes"] if record else []
        return_node_list = []
        if len(return_nodes) != 0:
            for node in return_nodes:
                out = {"ID": None, "type": None, "properties": None}
                out["ID"] = getattr(node, "element_id", None) or getattr(node, "id", None)
                out["type"] = next(iter(node.labels), None)
                node_type = out["type"]
                node_key_prop = model_parser.get_node_key_prop(node_name=node_type)
                out["properties"] = {}
                if node_key_prop:
                    out["properties"][node_key_prop] = node.get(node_key_prop)
                out["properties"][id_field] = node.get(id_field)
                return_node_list.append(out)
        else:
            pass
        return return_node_list