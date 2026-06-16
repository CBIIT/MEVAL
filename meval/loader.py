from time import time
import pandas as pd
from typing import Dict, Generator, Any, Tuple, Generator
from operator import itemgetter
from itertools import groupby
import os
from neo4j.exceptions import ClientError, ServiceUnavailable, TransientError
from neo4j_viz.neo4j import from_neo4j
from neo4j.graph import Path
from neo4j.time import DateTime
import json
import logging
from collections import defaultdict


class Loader:
    def __init__(self, driver: "GraphDatabase.driver"):
        self.driver = driver

    def close(self):
        self.driver.close()

    @staticmethod
    def serialize_datetime(dt: DateTime) -> str:
        """Serialize a Neo4j DateTime object to ISO format string.

        Args:
            dt (DateTime): The DateTime object to serialize.

        Returns:
            str: The ISO format string representation of the DateTime.
        """
        if isinstance(dt, DateTime):
            return dt.iso_format()
        else:
            return str(dt)

    @staticmethod
    def chunks(lst: list, size: int) -> Generator[list, None, None]:
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
                # dtype=str, # let pandas to infer data types
                encoding=encoding,
                chunksize=chunk_size,
                quotechar='"',
                doublequote=True,
                escapechar="\\",  # add escape char to handle special characters
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
        chunk: pd.DataFrame,
        model_parser: "ModelParser",
        subgraph_col: str | None = None,
        delimiter: str = ";",
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

        # get a list of list type properties (if any) in node == chunk_type
        list_type_props = model_parser.get_node_props_if_list_type(chunk_type)

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
            cleaned_record = {
                k: v for k, v in record.items() if k not in keys_to_remove
            }

            # if the propery is a list type, convert the value to list by a delimiter, such as ";"
            if len(list_type_props) > 0:
                for prop in cleaned_record:
                    if prop in list_type_props:
                        prop_value = cleaned_record[prop]
                        prop_list = []
                        if delimiter not in prop_value:
                            prop_list.append(prop_value.strip())
                        else:
                            prop_list = [
                                item.strip() for item in prop_value.split(delimiter)
                            ]
                        cleaned_record[prop] = prop_list
                    else:
                        pass

            # for remaining keys, if the cleaned_record value is an int/floar, check the model
            # we have cases of str type property that are mis inferred as number/int during loading
            for u in cleaned_record:
                if isinstance(cleaned_record[u], (int, float)):
                    expected_type = model_parser.get_prop_type(
                        node_name=chunk_type, prop_name=u
                    )
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
        tx,
        node_type: str,
        records: list[dict],
        id_field: str = "guid",
        logger: logging.Logger | None = None,
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
                if logger:
                    logger.error("Error upserting records: %s", e)
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
        delimiter: str = ";",
        logger: logging.Logger | None = None,
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
        if logger:
            logger.info(f"(Node Upsert) Start file {os.path.basename(file_path)}")
        print(f"(Node Upsert) Start file {os.path.basename(file_path)}")
        with self.driver.session() as tx:
            # with session.begin_transaction() as tx:
            for chunk in self.read_file_in_chunks(file_path, encoding, chunk_size):
                batch_count += 1
                if logger:
                    logger.info(f"Processing Batch {batch_count}...")
                print(f"Processing Batch {batch_count}...")
                chunk_type, records = self.generate_chunk_records(
                    chunk=chunk,
                    model_parser=model_parser,
                    subgraph_col=subgraph_col,
                    delimiter=delimiter,
                )
                result_summary = self.upsert_chunk_records_with_tx(
                    tx, chunk_type, records, id_field, logger=logger
                )
                if logger:
                    logger.info(f"Created {result_summary['nodes_created']} nodes")
                    logger.info(f"Set {result_summary['properties_set']} properties")
                    # logger.info("Batch loading time (seconds): %.2f", batch_end - batch_begin)
                print(f"Created {result_summary['nodes_created']} nodes")
                print(f"Set {result_summary['properties_set']} properties")
                # print("Batch loading time (seconds): ", batch_end - batch_begin)
                summary_list.append(result_summary)

        # combine counts in all summaries into one
        return_summary = defaultdict(int)
        for summary in summary_list:
            for key, value in summary.items():
                return_summary[key] += value
        return dict(return_summary)

    @staticmethod
    def generate_chunk_relationships(
        chunk: pd.DataFrame,
        model_parser: "ModelParser",
        id_field: str = "guid",
        delimiter: str = ";",
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
    def upsert_chunk_relationships_with_tx(
        tx, edge_list: list[dict], logger: logging.Logger | None = None
    ) -> dict:
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

                OPTIONAL MATCH (src:{src_label} {{{src_prop}: edge.src_match}})
                OPTIONAL MATCH (dst:{dst_label} {{{dst_prop}: edge.dst_match}})

                // Only create/update relationship when both exist
                FOREACH (_ IN CASE WHEN src IS NOT NULL AND dst IS NOT NULL THEN [1] ELSE [] END |
                    MERGE (src)-[r:{handle}]->(dst)
                    ON CREATE SET r.created = datetime()
                    ON MATCH  SET r.updated = datetime()
                )

                WITH edge, src, dst,
                    CASE
                        WHEN src IS NULL AND dst IS NULL THEN "missing_both"
                        WHEN src IS NULL THEN "missing_src"
                        WHEN dst IS NULL THEN "missing_dst"
                        ELSE "matched"
                    END AS status
                WHERE status <> "matched"
                RETURN edge.src_match as src_match, edge.dst_match as dst_match, status
                """
                params = {"edges": group}
                try:
                    results = ts.run(cypher, **params)
                    missing_pair = results.data()
                    # add src label and dst label to the missing_pair
                    # if there are missing pairs, we need to log them for trouble shooting
                    if len(missing_pair) > 0:
                        missing_pair_with_label = []
                        desired_key_order = [
                            "src_label",
                            "src_match",
                            "dst_label",
                            "dst_match",
                            "status",
                        ]
                        for item in missing_pair:
                            item["src_label"] = src_label
                            item["dst_label"] = dst_label
                            # reorder the keys in the item dictionary
                            item = {key: item[key] for key in desired_key_order}
                            missing_pair_with_label.append(item)
                        if logger:
                            logger.error(
                                "Rel upsert (%s)-[%s]->(%s) (partially) failed due to unmatched nodes:\n%s",
                                src_label,
                                handle,
                                dst_label,
                                json.dumps(
                                    missing_pair_with_label, indent=2, default=str
                                ),
                            )
                        print(
                            f"{len(missing_pair_with_label)} out of {len(group)} rels ({src_label})-[{handle}]->({dst_label}) failed to upsert due to unmatched nodes."
                        )
                        if logger:
                            logger.error(
                                f"{len(missing_pair_with_label)} out of {len(group)} rels ({src_label})-[{handle}]->({dst_label}) failed to upsert due to unmatched nodes"
                            )

                    else:
                        pass

                    summary = results.consume()

                    if logger:
                        logger.info(
                            f"Rels ({src_label})-[{handle}]->({dst_label}) created: {summary.counters.relationships_created}"
                        )
                    print(
                        f"Rels ({src_label})-[{handle}]->({dst_label}) created:",
                        summary.counters.relationships_created,
                    )
                    # print("print vars(summary.counters):")
                    # print(vars(summary.counters))
                    summary_list.append(vars(summary.counters))
                except Exception as e:
                    if logger:
                        logger.error("Error upserting records: %s", e)
                    print("Error upserting records: ", e)
                    ts.rollback()
                    raise e
        # combine counts in all summaries into one
        # print(json.dumps(summary_list, indent=2))
        return_summary = defaultdict(int)
        for summary in summary_list:
            for key, value in summary.items():
                return_summary[key] += value
        return dict(return_summary)

    @staticmethod
    def remove_chunk_duplicates(
        chunk: "pd.DataFrame",
        id_field: str = "guid",
        data_start_offset: int = 2,
        logger: logging.Logger | None = None,
    ) -> Tuple["pd.DataFrame", list]:
        """Remove duplicated row of a given id_field (e.g., guid) and report the removed row numbers

        Args:
            chunk (pd.DataFrame): a chunk read from submission file
            data_start_offset (int): Which line does the data start, defaults to 2
            id_field (str): The field/column name to check duplicates. Defaults to "guid"
            logger (logging.Logger | None, optional): The logger instance to use. Defaults to None.

        Returns:
            tuple: A tuple containing the updated chunk, a list of remaining row numbers, and a list of removed row numbers.
        """
        return_remove_list = []  # if no duplicates, return an empty list
        return_remain_list = []
        to_remove_index = chunk[chunk.duplicated(subset=[id_field], keep="last")]
        removed_indices = to_remove_index.index.tolist()
        if len(removed_indices) > 0:
            return_list = [data_start_offset + idx for idx in removed_indices]
            chunk.drop(index=to_remove_index.index, inplace=True)
            return_remove_list = return_list
            if logger:
                logger.warning(
                    f"Removed duplicated rows with the same {id_field} within the chunk. Row numbers (in the file) removed: {return_list}. Only keep the last occurrence in the chunk."
                )
            print(
                f"Removed duplicated rows with the same {id_field} within the chunk. Row numbers (in the file) removed: {return_list}. Only keep the last occurrence in the chunk."
            )
        else:
            pass
        # get the row number of remaining rows in the chunk,
        return_remain_list = [data_start_offset + idx for idx in chunk.index.tolist()]
        return chunk, return_remain_list, return_remove_list

    @staticmethod
    def read_tsv_at_row_number(file_path: str, row_number: int) -> dict:
        """Read a single row of a given file with a row number

        Args:
            file_path (str): The path to the file to read.
            row_number (int): The row number to read (1-based index).

        Returns:
            dict: A dictionary representing the row data.
        """
        encoding = Loader.check_encoding(file_path)
        if row_number < 2:
            raise ValueError(
                "Row number should be 2 or greater. Row 1 is expected to be the header."
            )
        try:
            row_data = pd.read_csv(
                file_path,
                sep="\t",
                encoding=encoding,
                skiprows=lambda x: x != 0
                and x != (row_number - 1),  # skip rows before the target row
                nrows=1,  # read only one row
                quotechar='"',
                doublequote=True,
                escapechar="\\",  # add escape char to handle special characters
                keep_default_na=False,
                na_values=[""],  # treat empty strings as NaN
            )
            return row_data.to_dict(orient="records")[
                0
            ]  # return the single row as a dict
        except Exception as e:
            print(f"Error reading row {row_number} from {file_path}: {e}")
            raise e

    @staticmethod
    def generate_del_rel_list_of_a_record(
        record_dict: dict, id_field: str = "guid", delimiter: str = ";"
    ) -> list[dict]:
        """

        Args:
            record_dict (dict): a record dictionary from function read_tsv_at_row_number
            id_field (str, optional): id field. Defaults to "guid".
            delimiter (str, optional): delimiter used in the record. Defaults to ";".

        Returns:
            list[dict]: A list of dictionaries representing the relationships to be deleted.
        """
        edge_keys = [key for key in record_dict.keys() if "." in key]
        rel_list = []
        for edge in edge_keys:
            edge_parent, edge_parent_prop = edge.split(".")
            if delimiter not in record_dict[edge]:
                rel_item = {}
                rel_item["src_label"] = record_dict["type"]
                rel_item["src_prop"] = id_field
                rel_item["src_match"] = record_dict[id_field]
                rel_item["dst_label"] = edge_parent
                rel_item["dst_prop"] = edge_parent_prop
                rel_item["dst_match"] = record_dict[edge]
                rel_list.append(rel_item)
            else:
                dst_matches = record_dict[edge].split(delimiter)
                for dst in dst_matches:
                    rel_item = {}
                    rel_item["src_label"] = record_dict["type"]
                    rel_item["src_prop"] = id_field
                    rel_item["src_match"] = record_dict[id_field]
                    rel_item["dst_label"] = edge_parent
                    rel_item["dst_prop"] = edge_parent_prop
                    rel_item["dst_match"] = dst.strip()
                    rel_list.append(rel_item)
        return rel_list

    def remove_rel_of_record(
        self, rel_list: list[dict], logger: logging.Logger | None = None
    ) -> dict:
        """
        Delete relationships for a batch of node pairs.

            Expected input format:
            [
                {
                    "src_label": "image",
                    "src_prop": "guid",
                    "src_match": "805be67f-c9e4-51cb-939e-c9fd6bb9cd71",
                    "dst_label": "file",
                    "dst_prop": "guid",
                    "dst_match": "287f3775-4f4b-50f4-95ec-555191fe2011"
                }
            ]

            Returns:
                dict with per-batch counters
        """
        # Group rows by Cypher shape so we can batch them efficiently.
        grouped = defaultdict(list)
        for item in rel_list:
            key = (
                item["src_label"],
                item["src_prop"],
                item["dst_label"],
                item["dst_prop"],
            )
            grouped[key].append(
                {
                    "src_match": item["src_match"],
                    "dst_match": item["dst_match"],
                }
            )

        counters_list = []
        with self.driver.session() as session:
            for (src_label, src_prop, dst_label, dst_prop), pairs in grouped.items():
                # This cypher query won't return any unmatched rel, if eitehr src or dst node is missing, there won't be a match and thus no delete
                # it also means, when this record was procecssed before, there wasn't any relationship established either
                cypher = f"""
                UNWIND $pairs AS pair
                MATCH (src:{src_label} {{{src_prop}: pair.src_match}})
                MATCH (dst:{dst_label} {{{dst_prop}: pair.dst_match}})
                MATCH (src)-[r]->(dst)
                DELETE r
                """
                try:
                    result = session.run(cypher, pairs=pairs)
                    counters_list.append(vars(result.consume().counters))
                except Exception as e:
                    if logger:
                        logger.error("Error deleting relationships: %s", e)
                    else:
                        print("Error deleting relationships: ", e)
                    raise e
        # combine counts in all summaries into one
        return_summary = defaultdict(int)
        for summary in counters_list:
            for key, value in summary.items():
                return_summary[key] += value
        return dict(return_summary)

    @staticmethod
    def turn_remain_row_list_to_dict(
        chunk: "pd.DataFrame",
        file_path: str,
        remain_row_list: list,
        id_field: str = "guid",
    ) -> dict:
        """Generates a dict which records each record's source from a chunk
        The dictionary will be in the format like this:
        {
            "guid_value":{
                "file_path": "source/file/path",
                "row_number": 10
                ""
            }
        }

        Args:
            chunk (pd.DataFrame): a chunk from the source file
            file_path (str): source file path
            remain_row_list (list): The list of row numbers of these records

        Returns:
            dict: a dictionary of chunk records keeping
        """
        return_dict = {}
        # chunk and remain_row_list should be the same lentgh
        count = 0
        for _, row in chunk.iterrows():
            guid_row = row[id_field]
            return_dict[guid_row] = {
                "file_path": file_path,
                "row_number": remain_row_list[count],
            }
            count += 1
        return return_dict

    def upsert_file_relationships(
        self,
        file_path: str,
        model_parser: "ModelParser",
        processed_rel_dict: dict,
        id_field: str = "guid",
        chunk_size: int = 3000,
        delimiter: str = ";",
        logger: logging.Logger | None = None,
    ) -> Tuple[dict, dict]:
        """Upsert relationships of a given file
        Relationships can only be done when both parent and child nodes have been created

        Args:
            file_path (str): The path to the file to process.
            id_field (str, optional): The name of the ID field. Defaults to "guid".
            chunk_size (int, optional): The size of the chunks to process. Defaults to 3000.
            delimiter (str, optional): The delimiter used in the file. Defaults to ";".
            logger (logging.Logger, optional): The logger instance to use. Defaults to None.

        Returns:
            tuple[dict, dict]: A tuple of (summary dict, updated processed_rel_dict).
        """
        encoding = self.check_encoding(file_path)
        summary_list = []
        batch_count = 0

        # Use a single session but separate transactions for each chunk
        if logger:
            logger.info(
                f"(Rel Upsert) Start processing file {os.path.basename(file_path)}"
            )
        print(f"(Rel Upsert) Start processing file {os.path.basename(file_path)}")
        with self.driver.session() as tx:
            data_start_offset = 2  # data line starts at line 2
            for chunk in self.read_file_in_chunks(file_path, encoding, chunk_size):
                batch_count += 1
                if logger:
                    logger.info(f"Processing batch {batch_count}...")
                print(f"Processing batch {batch_count}...")

                # remove duplicated records within the chunk based on id_field, e.g., guid
                chunk, remain_list, remove_list = self.remove_chunk_duplicates(
                    chunk=chunk,
                    id_field=id_field,
                    data_start_offset=data_start_offset,
                    logger=logger,
                )
                if len(remove_list) > 0:
                    if logger:
                        logger.info(
                            f"Batch {batch_count} removed {len(remove_list)} duplicated rows based on {id_field}. Row numbers (in the file) removed: {remove_list}."
                        )
                    print(
                        f"Batch {batch_count} removed {len(remove_list)} duplicated rows based on {id_field}. Row numbers (in the file) removed: {remove_list}."
                    )
                else:
                    if logger:
                        logger.info(
                            f"Batch {batch_count} has no duplicated rows based on {id_field}."
                        )
                    print(
                        f"Batch {batch_count} has no duplicated rows based on {id_field}."
                    )

                # keep track of rows to be processed
                processed_rel_chunk_records = self.turn_remain_row_list_to_dict(
                    chunk, file_path, remain_list, id_field=id_field
                )
                for guid, source in processed_rel_chunk_records.items():
                    if guid not in processed_rel_dict:
                        processed_rel_dict[guid] = source
                    else:
                        # guid found in the processed_rel_dict
                        # delete the old established relationships
                        guid_old_source = processed_rel_dict[guid]
                        if logger:
                            logger.warning(
                                f"guid {guid} have been processed in the current loading job. Deleting the previous established relationships and replacing with the one from the current chunk"
                            )
                            logger.warning(
                                f"guid {guid} was processed in {os.path.basename(guid_old_source['file_path'])} at row {guid_old_source['row_number']}. Deleting the relationships established by this record (if any)."
                            )
                        print(
                            f"guid {guid} have been processed in the current loading job. Deleting the previous established relationships and replacing with the one from the current chunk"
                        )
                        print(
                            f"guid {guid} was processed in {os.path.basename(guid_old_source['file_path'])} at row {guid_old_source['row_number']}. Deleting the relationships established by this record (if any)."
                        )

                        guid_old_source_record = self.read_tsv_at_row_number(
                            file_path=guid_old_source["file_path"],
                            row_number=guid_old_source["row_number"],
                        )
                        guid_old_rel_list = self.generate_del_rel_list_of_a_record(
                            record_dict=guid_old_source_record,
                            id_field=id_field,
                            delimiter=delimiter,
                        )
                        if len(guid_old_rel_list) > 0:
                            # even with guid_old_rel_list >0, there still might not be any relationship to delete if the previous processing didn't establish any relationship when a src or dst node not found
                            del_rel_summary = self.remove_rel_of_record(
                                rel_list=guid_old_rel_list, logger=logger
                            )
                            if logger:
                                logger.warning(
                                    f"Relationships deleted: {del_rel_summary.get('relationships_deleted', 0)}"
                                )
                            print(
                                f"Relationships deleted: {del_rel_summary.get('relationships_deleted', 0)}"
                            )
                            summary_list.append(del_rel_summary)
                        else:
                            if logger:
                                logger.warning(f"Relationships deleted: 0")
                            print(f"Relationships deleted: 0")
                        processed_rel_dict[guid] = (
                            source  # replace the source info using the guid information of the current chunk
                        )

                chunk_relationships = self.generate_chunk_relationships(
                    chunk=chunk,
                    id_field=id_field,
                    model_parser=model_parser,
                    delimiter=delimiter,
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
                                tx, edge_list=chunk_relationships, logger=logger
                            )
                            summary_list.append(summary)
                            if logger:
                                logger.info(f"Batch {batch_count} completed")
                            print(f"Batch {batch_count} completed")
                            break  # Success, exit retry loop
                        except Exception as e:
                            retry_count += 1
                            if logger:
                                logger.warning(
                                    f"Batch {batch_count} failed (attempt {retry_count}/{max_retries}): {e}"
                                )
                            print(
                                f"Batch {batch_count} failed (attempt {retry_count}/{max_retries}): {e}"
                            )
                            if retry_count >= max_retries:
                                if logger:
                                    logger.error(
                                        f"Batch {batch_count} failed after {max_retries} attempts, skipping..."
                                    )
                                print(
                                    f"Batch {batch_count} failed after {max_retries} attempts, skipping..."
                                )
                                # Optionally re-raise the exception or continue
                                raise e
                            else:
                                if logger:
                                    logger.warning(f"Retrying batch {batch_count}...")
                                print(f"Retrying batch {batch_count}...")
                else:
                    if logger:
                        logger.info(
                            f"Batch {batch_count} skipped: no relationships to create"
                        )
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
            }, processed_rel_dict
        else:
            # combine counts in all summaries into one
            return_summary = defaultdict(int)
            for summary in summary_list:
                for key, value in summary.items():
                    return_summary[key] += value
            return dict(return_summary), processed_rel_dict

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
        Delete in batches, committing each batch independently.
        Can not rollback when error happens in the middle, but it is more efficient and less likely to run into transaction timeout issue than deleting all at once for large database.
        """
        deleted_nodes = 0
        with self.driver.session() as session:
            while True:
                cypher = """
                MATCH (n)
                WITH n LIMIT $batch_size
                DETACH DELETE n
                RETURN count(n) AS deleted_count
                """
                try:
                    result = session.run(cypher, batch_size=batch_size)
                    deleted_count = result.single()["deleted_count"]
                    deleted_nodes += deleted_count
                    if deleted_count == 0:
                        break
                except Exception as e:
                    print("Error wiping database: ", e)
                    raise e
        return deleted_nodes

    def wipe_rooted_subgraph(
        self,
        root_node_label: str,
        root_node_prop: str,
        subgraph_value: str,
        chunk_threshold: int = 10000,
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
            print(
                f"No nodes found in root node {root_node_label} with {root_node_prop}={subgraph_value}. Nothing to delete."
            )
        # found nodes belonging to the subgraph
        else:
            print(
                f"{len(node_ids)} nodes found in root node {root_node_label} with {root_node_prop}={subgraph_value}. Start deleting..."
            )
            if len(node_ids) > chunk_threshold:
                # large subgraph to delete, print warning and delete in batches
                print(
                    f"Warning: large subgraph detected with {len(node_ids)} nodes, deletion may take a while..."
                )
                # first detach relationships in batches
                print("Start detaching relationships...")
                rel_batch_count = 0
                for batch in self.chunks(
                    node_ids, 5000
                ):  # chunk it for 5000 by default
                    rel_batch_count += 1
                    detached_nodes_count = self._detach_nodes_by_ids(node_ids=batch)
                    print(
                        f"Detached rels count for batch {rel_batch_count}: ",
                        detached_nodes_count,
                    )
                    detached_rels += detached_nodes_count
                print(f"Total relationships detached: {detached_rels}")
                # then delete nodes in batches
                print("Start deleting nodes...")
                node_batch_count = 0
                for batch in self.chunks(
                    node_ids, 5000
                ):  # chunk it for 5000 by default
                    node_batch_count += 1
                    deleted_nodes_count = self._delete_nodes_by_ids(node_ids=batch)
                    print(
                        f"Deleted nodes count for batch {node_batch_count}: ",
                        deleted_nodes_count,
                    )
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

    def _get_rooted_subgraph_nodes_ids(
        self, root_node_label: str, root_node_property: str, subgraph_value: str
    ) -> list[int]:
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
        viz_filename: str | None = "intermediate_anchored_traversals.html",
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
            print(
                "Warning: large graph deteted with more than 1000 nodes, graph too large to render safely."
            )
            return None
        else:
            VG = from_neo4j(graph_obj)
            html_obj = VG.render(layout="forcedirected", height="900px")
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
    def cypher_path_to_compact_dict(
        p: Path, model_parser: "ModelParser", id_field: str = "guid"
    ) -> Dict[str, Any]:
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
                out["ID"] = getattr(node, "element_id", None) or getattr(
                    node, "id", None
                )
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

    def find_nodes_without_path_to_root(
        self,
        root_node_label: str,
    ) -> Generator[dict[str, Any], None, None]:
        """Find nodes that do not have a path to any root node. A common use case is to find nodes that are not connected to any study/program node in the graph, which means they are orphan nodes or they are connected to some other nodes but the path to root node is missing in the graph database.

        Args:
            root_node_label (str): The label of the root node, such as "study" or "program". Root nodes deosn't have OUTGOING relationship to any other nodes.

        Returns:
            Generator[dict[str, Any], None, None]: A generator of dictionaries representing the nodes without path to root.
        """
        query = f"""
        MATCH (n)
        WHERE NOT n:{root_node_label}
        AND NOT EXISTS ((n)-[*]->(:{root_node_label}))
        RETURN n
        """
        with self.driver.session() as session:
            result = session.run(query)
            for record in result:
                node = record["n"]
                db_internal_id = getattr(node, "id", None) or getattr(
                    node, "element_id", None
                )
                yield {
                    "db_internal_id": db_internal_id,
                    "type": next(iter(node.labels), None), # there should be only one label of a data node
                    "properties": {k: node.get(k) for k in node.keys()}
                }

    def delete_nodes_by_internal_id(
        self, identifier_list: list[str], batch_size: int = 5000
    ) -> int:
        """Delete nodes by internal node ID using `id(n)` in batches.

        Args:
            identifier_list (list[str]): Internal node IDs as strings, e.g. ["1", "42"].
            batch_size (int, optional): Maximum number of IDs to delete per query.
                Defaults to 5000.

        Returns:
            int: Number of nodes deleted.

        Raises:
            ValueError: If any identifier is not a valid integer.
            ValueError: If batch_size is less than 1.
            ClientError: If Neo4j query execution fails.
        """
        if not identifier_list:
            return 0

        if batch_size < 1:
            raise ValueError("batch_size must be >= 1")

        invalid_ids = []
        node_ids = []
        for identifier in identifier_list:
            try:
                node_ids.append(int(identifier))
            except (TypeError, ValueError):
                invalid_ids.append(identifier)

        if invalid_ids:
            raise ValueError(
                f"All identifiers must be numeric internal IDs for either Neo4j 4+ or Memgraph instance. Invalid values: {invalid_ids}"
            )

        query = """
        UNWIND $node_ids AS nid
        MATCH (n)
        WHERE id(n) = nid
        WITH DISTINCT n
        DETACH DELETE n
        RETURN count(n) AS deleted_nodes
        """
        total_deleted = 0
        with self.driver.session() as session:
            for node_ids_batch in self.chunks(node_ids, batch_size):
                result = session.run(query, node_ids=node_ids_batch)
                record = result.single()
                total_deleted += record["deleted_nodes"] if record else 0
        return total_deleted

    def delete_nodes_by_prop_value(
        self, identifier_list: list[str], property_name: str, batch_size: int = 5000
    ) -> int:
        """Delete nodes by property value in batches. We expect this property to be a universal unique identifier (UUID) property, such as "guid", or "id".

        Args:
            identifier_list (list[str]): A list of property values to match for deletion, e.g. ["uuid1", "uuid2"].
            property_name (str): The property name to match.
            batch_size (int, optional): Maximum number of nodes to delete per query.
                Defaults to 5000.

        Returns:
            int: Number of nodes deleted.

        Raises:
            ValueError: If batch_size is less than 1.
            ClientError: If Neo4j query execution fails.
        """
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1")

        query = f"""
        UNWIND $property_value AS prop_val
        MATCH (n)
        WHERE n.{property_name} = prop_val
        WITH DISTINCT n
        DETACH DELETE n
        RETURN count(n) AS deleted_nodes
        """
        total_deleted = 0
        with self.driver.session() as session:
            for property_value_batch in self.chunks(identifier_list, batch_size):
                result = session.run(query, property_value=property_value_batch)
                record = result.single()
                total_deleted += record["deleted_nodes"] if record else 0
        return total_deleted

    def check_unique_node(self, property_value: str, property_name: str = "guid") -> Tuple[bool, dict]:
        """Check if a node with the given property(usually guid) name and value exists in the database, and if yes, if it is unique.
        We only expect this function to be used for checking the uniqueness of a data node through a guid value
        If exactly one node exists with the given property value, return True; if no node exist or more than one node exist with the given property value, return False. The second item in the returned tuple is a dictionary containing details of failure.

        Args:
            property_name (str): The property name to check, e.g. "guid".
            property_value (str): The property value to check, e.g. "uuid1".

        Returns:
            Tuple[bool, dict]: A tuple containing
                - A boolean indicating if the node is unique (True if exactly one node exists with the given property value, False if not exist or more than one node exists)
                - A dictionary containing details of failure.
        """
        query = f"""
        MATCH (n)
        WHERE n.{property_name} = $property_value
        RETURN collect(n) AS nodes
        """
        with self.driver.session() as session:
            result = session.run(query, property_value=property_value)
            record = result.single()
            nodes = record["nodes"] if record else []
            return_nodes = [
                {
                    "labels": list(node.labels),  # frozenset → list
                    "properties": {
                        k: self.serialize_datetime(v) for k, v in dict(node).items()
                    },
                }
                for node in nodes
            ]
            if len(nodes) == 1:
                return True, {"check_item": "node uniqueness", "check_result": "Pass", "message": f"One unique node found with {property_name}={property_value}", "matched_node(s)": return_nodes}
            elif len(nodes) == 0:
                return False, {
                    "check_item": "node uniqueness",
                    "check_result": "Fail",
                    "message": f"No node found with {property_name}={property_value}",
                    "matched_node(s)": [],
                }
            else:
                return False, {"check_item": "node uniqueness", "check_result": "Fail", "message": f"Multiple nodes found with {property_name}={property_value}", "matched_node(s)": return_nodes}

    def find_upstream_nodes(self, property_value: str, property_name: str = "guid") -> list[dict[str, Any]]:
        """Find all upstream nodes directly or indirectly connected to the node with the given property name and value. 
        We only expect this function to use uuid property for finding upsteam nodes of a target node. 
        We EXPECT the target node is a UNIQUE node.

        Args:
            property_name (str): The property name to match, e.g. "guid".
            property_value (str): The property value to match, e.g. "uuid1".

        Returns:
            list[dict[str, Any]]: A list of dictionaries representing the upstream nodes. Each dictionary contains the internal ID, type, and properties of an upstream node.
        """
        query = f"""
        MATCH (m)
        WHERE m.{property_name} = $property_value
        MATCH (n)-[*]->(m)
        RETURN collect(DISTINCT n) AS upstream_nodes
        """
        with self.driver.session() as session:
            result = session.run(query, property_value=property_value)
            record = result.single()
            upstream_nodes = record["upstream_nodes"] if record else []
            return_nodes = [
                {
                    "labels": list(node.labels),  # frozenset → list
                    "properties": {
                        k: self.serialize_datetime(v) for k, v in dict(node).items()
                    },
                }
                for node in upstream_nodes
            ]
            return return_nodes # if target is leaf node, it return an emtpy list

    def if_alternative_path_to_root(self, property_name: str, target_property_value: str, node_to_avoid_property_value: str, root_label: str) -> bool:
        """Find if there is an alternative path from a node to a root labeled node that DOES NOT go through a node (of interest). 
        A common use case is to check if a upstream/child node of a target node (aka, node to avoid) can reach root node through an alternative path.
        An upstream/child node of a target node is usually recommeneded to be deleted along with the target node. But if there is an alternative path between child node and root node, it means that child node may have multiple outgoing edges to multiple parent nodes, and it requires closer inspection.

        Args:
            property_name (str): The property name to match, e.g. "guid".
            target_property_value (str): The property value of the target node, e.g. "uuid1".
            node_to_avoid_property_value (str): The property value of the node to avoid, e.g. "uuid2".
            root_label (str): The label of the root node, e.g. "study".

        Returns:
            bool: True if there is an alternative path from the target node to any root node that doesn't go through the node to avoid, False otherwise.
        """
        query = f"""
        MATCH (target)
        WHERE target.{property_name} = $target_property_value
        MATCH (node_to_avoid)
        WHERE node_to_avoid.{property_name} = $node_to_avoid_property_value
        MATCH p = (target)-[*]->(root:{root_label})
        WHERE NOT node_to_avoid IN nodes(p)
        RETURN count(p) AS alternative_paths_count
        """
        with self.driver.session() as session:
            result = session.run(
                query,
                target_property_value=target_property_value,
                node_to_avoid_property_value=node_to_avoid_property_value,
            )
            record = result.single()
            alternative_paths_count = record["alternative_paths_count"] if record else 0
            return alternative_paths_count > 0
