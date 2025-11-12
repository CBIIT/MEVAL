import pandas as pd
from typing import Generator
from operator import itemgetter
from itertools import groupby
import os


class Loader:
    def __init__(self, driver: "GraphDatabase.driver"):
        self.driver = driver

    def close(self):
        self.driver.close()

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
                dtype=str,
                encoding=encoding,
                chunksize=chunk_size,
                quotechar='"',
                doublequote=True,
                keep_default_na=False,
            )
            for chunk in reader:
                # which row contains data node properties as well as relationships
                yield chunk
        except Exception as e:
            print(f"Error reading {file_path}: {e}")
            raise e

    @staticmethod
    def generate_chunk_records(
        chunk: pd.DataFrame, subgraph_col: str|None = None
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
            # ensure all values are strings
            for key in record:
                # convert NaN to empty string
                if pd.isna(record[key]):
                    record[key] = ""
                else:
                    record[key] = str(record[key])
            records.append(record)
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
        all_keys = set()
        for record in records:
            all_keys.update(record.keys())
        all_keys = list(all_keys)
        set_clause = ", ".join([f"n.{k} = record.{k}" for k in all_keys])
        cypher_statement = f"""
        UNWIND $records AS record
        MERGE (n:{node_type} {{ {id_field}: record.{id_field} }})
        ON CREATE SET {set_clause}, n.created = dateTime()
        ON MATCH SET {set_clause}, n.updated = dateTime()
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
        subgraph_col: str|None = None,
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
                chunk_type, records = self.generate_chunk_records(chunk, subgraph_col)
                result_summary = self.upsert_chunk_records_with_tx(
                    tx, chunk_type, records, id_field
                )
                print(
                    f"Batch {batch_count} created {result_summary['nodes_created']} nodes"
                )
                print(
                    f"Batch {batch_count} set {result_summary['properties_set']} properties"
                )
                summary_list.append(result_summary)

        # combine counts in all summaries into one
        return_summary = {key: 0 for key in summary_list[0].keys()}
        for summary in summary_list:
            for key, value in summary.items():
                return_summary[key] += value
        return return_summary

    @staticmethod
    def generate_chunk_relationships(
        chunk: pd.DataFrame, model_parser: "ModelParser", id_field: str = "guid"
    ) -> list[dict]:
        """
        Generate a list of relationship records from a chunk of data.

        Args:
            chunk (pd.DataFrame): The chunk of data to process.
            id_field (str, optional): The unique identifier field. Defaults to "guid".
            model_parser (ModelParser): The model parser instance to use.

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
            edge_parent = edge.split(".")[0]
            edge_parent_prop = edge.split(".")[1]
            # this is the edge handle/label
            edge_handle = model_parser.get_edge_handle(
                edge_src=chunk_type, edge_dst=edge_parent
            )
            # extract only two columns, id_field and edge
            # remove rows if edge is empty
            chunk_filtered = chunk[chunk[edge] != ""]
            chunk_filtered = chunk_filtered[[id_field, edge]]
            # if there is edge left to establish
            if chunk_filtered.shape[0] > 0:
                edges_list = chunk_filtered.to_dict(orient="records")
                for item in edges_list:
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
                    chunk=chunk, id_field=id_field, model_parser=model_parser
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

    def wipe_subgraph(
        self, root_node_label: str, root_node_prop: str, subgraph_value: str
    ) -> int:
        """Wipe a subgraph by deleting all nodes and relationships connected to a root node."""
        # Helper: generator to split list into batches
        def chunks(lst, size):
            for i in range(0, len(lst), size):
                yield lst[i:i + size]

        delete_nodes = 0
        with self.driver.session() as session:
            with session.begin_transaction() as tx:
                # identify all node ids in the subgraph
                id_result = tx.run(
                    f"""
                MATCH (s:{root_node_label} {{{root_node_prop}: $subgraph_value}})
                OPTIONAL MATCH (s)-[*]-(n)
                WITH COLLECT(DISTINCT id(s)) + COLLECT(DISTINCT id(n)) AS node_ids
                RETURN node_ids
                """,
                    subgraph_value=subgraph_value,
                ).single()
                node_ids = id_result["node_ids"] or []
                total_nodes = len(node_ids)
                print(f"Total nodes to delete in subgraph {subgraph_value}: {total_nodes}")

                # start deleting nodes in batches
                batch_count = 0
                for batch in chunks(node_ids, 5000):
                    batch_count += 1
                    print(f"Processing batch {batch_count}: {len(batch)}")
                    try:
                        res = tx.run(f"""
                        MATCH (n)
                        WHERE id(n) IN $batch
                        DETACH DELETE n
                        RETURN count(n) AS deleted
                        """, batch=batch
                        ).single()
                        deleted_count = res["deleted"]
                        delete_nodes += deleted_count
                        if deleted_count == 0:
                            break
                    except Exception as e:
                        print(f"Error wiping subgraph {subgraph_value}: ", e)
                        tx.rollback()
                        raise e
        print(f"Total nodes deleted in subgraph {subgraph_value}: {delete_nodes}")
        return delete_nodes
