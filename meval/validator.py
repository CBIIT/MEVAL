from bento_mdf import MDFDataValidator, MDFReader, MDF
from typing import Any, List
from pathlib import Path, PosixPath
import csv
import os
from neo4j import GraphDatabase
import pandas as pd
from uuid import UUID, uuid5
import hashlib
from typing import Literal, Tuple
from collections.abc import Iterator
from meval.parser import ModelParser
from itertools import islice
from collections import defaultdict
import re

# for validating record from a submission file against db
TestModeList = Literal["New", "Update", "Upsert"]
# Mode New is to test if the record exist in the db. It should return error if the record already exist in the db
# Update is to test if the record can be updated in the db. The tested record should exist in the db
# Upsert is to test if the record can be either created as new record or updated if it already exist in the db. Setting property values using Merge statement doesn't wipe off exisitng property values

CompareRecordMode = Literal["Update", "Upsert"]


class Validator:
    def __init__(self, mdf: MDFReader):
        self.mdf = mdf
        self.model = self.mdf.model
        self.record_validator = MDFDataValidator(mdf=self.mdf)

    @staticmethod
    def to_number(value: str) -> float | int | str:
        """Convert a string value (read through csv.DictReader) to a number (int or float) when the property type(value_domain or item domain for a list) is a number/integer based on the model.
        Value can not be None or empty string
        The function doesn't convert the display of the value. If the value can not be converted to number, it will stay as string
        """
        try:
            if "." in value:
                return float(value)
            else:
                return int(value)
        except ValueError:
            return value

    @staticmethod
    def record_comparison(
        record_file: dict, record_db: dict, compare_mode: CompareRecordMode
    ):
        """
        ########################
        # FOR VALIDATION IN DB #
        ########################
        Compared a record in file with another record in db (sharing the same id value, such as guid)
        record_file should be a return from Validator.read_record_by_row_in_tsv,
        record_db should be a return from Validator.get_node_record_in_db
        Comparison between "Upsert" and "Update":
            - Upsert: if the key in the record_file is not in the record_db, record in db won't delete that key after Upsert loading.
            - Update: if the key in the record_file is not in the record_db, record in db will delete that key after Update loading

        Args:
            record_file (dict): record dictionary from the file.
            record_db (dict): record dictionary from the db
            compare_mode (CompareRecordMode): "Update" or "Upsert".
        """
        added = {k: record_file[k] for k in record_file.keys() - record_db.keys()}
        removed = {k: record_db[k] for k in record_db.keys() - record_file.keys()}
        changed = {
            k: {"record_in_file": record_file[k], "record_in_db": record_db[k]}
            for k in record_file.keys() & record_db.keys()
            if record_file[k] != record_db[k]
        }
        # if compare_mode is "Upsert", nothing will be removed from removed dict since they won't be deleted in the db after upsert loading
        if compare_mode == "Upsert":
            comparison_result = {"added": added, "removed": {}, "changed": changed}
        elif compare_mode == "Update":
            comparison_result = {"added": added, "removed": removed, "changed": changed}
        else:
            raise ValueError(
                f"Invalid compare_mode: {compare_mode}. Must be 'Update' or 'Upsert'."
            )
        return comparison_result

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
    def get_project_namespace(project_name: str) -> str:
        """
        Get the namespace of a project, which can be used for uuid5 generation

        Returns:
            UUID: UUID instance
        """
        hex_string = hashlib.md5(project_name.encode("UTF-8")).hexdigest()
        project_namespace = UUID(hex=hex_string)
        return project_namespace

    @staticmethod
    def generate_uuid5(
        project_name: str,
        subgraph_value: str,
        record_type: str,
        record_key_value: str,
        delimiter: str | None = ";",
    ) -> str:
        """
        Generate a UUID5 based on the project namespace, record type, and record key value.

        Args:
            project_name (str): The name of the project.
            record_type (str): The type of the record. e.g. "participant", "diagnosis", etc.
            record_key_value (str): The key value of the record. e.g. the value of "participant_id" column for a participant record, or the value of "diagnosis_id" for a diagnosis record.

        Returns:
            str: The generated UUID5 as a string.
        """
        str_key_value = str(record_key_value)
        if pd.isna(record_key_value) or str_key_value.strip() == "":
            return ""
        else:
            if delimiter is None:
                project_namespace = Validator.get_project_namespace(project_name)
                str_input = f"{subgraph_value}::{record_type}::{str_key_value}"
                str_uuid = uuid5(project_namespace, str_input)
                return str(str_uuid)
            else:
                str_key_list = [
                    str(item).strip() for item in str_key_value.split(delimiter)
                ]
                return_uuid_list = []
                for item in str_key_list:
                    if pd.isna(item) or item.strip() == "":
                        pass
                    else:
                        project_namespace = Validator.get_project_namespace(
                            project_name
                        )
                        str_input = f"{subgraph_value}::{record_type}::{item}"
                        str_uuid = uuid5(project_namespace, str_input)
                        return_uuid_list.append(str(str_uuid))
                return_uuid = delimiter.join(return_uuid_list)
                return return_uuid

    @staticmethod
    def add_uuid_to_tsv_file(
        file_path: str | Path,
        project_name: str,
        mdf: MDF,
        output_file_path: str,
        uuid_column: str = "guid",
        delimiter: str | None = ";",
        subgraph_value: str | None = None,
    ) -> None:
        """
        Add a "guid" column to the TSV file with generated UUID5 values based on the project namespace, record type, and record key value.
        The output TSV will have the UUID column, such as "guid", and all relationship columns will be converted to UUID based on the parent type.
        The original relationship columns and the "subgraph" column will be removed from the output TSV.

        Args:
            file_path (str): The path to the input TSV file.
            project_name (str): The acrynom of the commons, e.g. "ccdi", "icdc", "cds", "c3dc", "ctdc", "ccdi_dcc", "popsci". This is used to generate project namespace for uuid5 generation.
            mdf (MDF): The MDF object generates from data model files.
            output_file_path (str): The path to the output TSV file with uuid column added.
            uuid_column (str): The name of the uuid column to be added, default is "guid"
            delimiter (str | None): The delimiter to use for splitting multiple key values. Defaults to ";".
            subgraph_value (str | None): The value of a subgraph key, such as "phs000123", which is also used for uuid generation. If not provided, the function will look for a "subgraph" column in the TSV file.

        """
        file_path_str = str(file_path)
        encoding = Validator.check_encoding(file_path_str)
        file_df = pd.read_csv(
            file_path_str,
            sep="\t",
            encoding=encoding,
            quotechar='"',
            doublequote=True,
            escapechar="\\",  # add escape char to handle special characters
            keep_default_na=False,
            na_values=[""],  # treat empty strings as NaN
            dtype=str,  # read columns as str. This is to avoid infer columns full of numbers as float64
        )
        file_type = file_df["type"].iloc[0]  # we only expect one type of a file

        # test if the subgraph column has any NA or more than one unique value
        if (
            subgraph_value is None
        ):  # if no subgraph_value is provided, we will look for column "subgraph" in the file"
            if "subgraph" not in file_df.columns:
                raise KeyError(
                    f"No subgraph column found in {file_path_str}, unable to generate uuid without subgraph value for namespace, please add a subgraph column with value"
                )
            elif file_df["subgraph"].isna().any():
                raise ValueError(
                    f"subgraph column in {file_path_str} contains NA value, unable to generate uuid without subgraph value for namespace, please add any missing subgraph value"
                )
            elif file_df["subgraph"].nunique() > 1:
                raise ValueError(
                    f"subgraph column in {file_path_str} contains more than one unique value. Only one unique subgraph value is expeted per file. Subgraph values found: {",".join(file_df["subgraph"].unique().tolist())}"
                )
            else:
                # take the subgraph value from the first record
                file_subgraph_value = file_df["subgraph"].iloc[0]
        else:
            file_subgraph_value = subgraph_value

        # check if subgraph value is empty string or stirng with whitespace only
        if pd.isna(file_subgraph_value) or file_subgraph_value.strip() == "":
            raise ValueError(
                f"No subgraph value found in {file_path_str} at first record, unable to generate uuid without subgraph value for namespace, please add a subgraph value"
            )
        else:
            pass

        file_type_key_prop = mdf.model.nodes[file_type].get_key_prop().handle

        # first write uuid column
        # uuid column for the file type itself don't need delimiter since it's expected to be only one key value for each record
        file_df[uuid_column] = file_df.apply(
            lambda row: Validator.generate_uuid5(
                project_name=project_name,
                subgraph_value=file_subgraph_value,
                record_type=file_type,
                record_key_value=row[file_type_key_prop],
                delimiter=None,
            ),
            axis=1,
        )

        # second write guid for all relationship columns
        # relationship column might need delimiter if it is present
        rel_col = [col for col in file_df.columns if "." in col]
        for col in rel_col:
            parent_type = col.split(".")[0]
            new_rel_col = parent_type + "." + uuid_column
            # I'll leave delimiter available here for all multiplicity types. Because There might be use cases of project wanting to have many_to_many or one_to_many relationship before they were able to release model
            file_df[new_rel_col] = file_df.apply(
                lambda row: Validator.generate_uuid5(
                    project_name=project_name,
                    subgraph_value=file_subgraph_value,
                    record_type=parent_type,
                    record_key_value=row[col],
                    delimiter=delimiter,
                ),
                axis=1,
            )

        # remove original relationship columns and subgraph column
        cols_to_remove = (
            rel_col + ["subgraph"] if "subgraph" in file_df.columns else rel_col
        )
        file_df.drop(columns=cols_to_remove, inplace=True)
        # write to new file in the given output file path
        file_df.to_csv(output_file_path, sep="\t", index=False)
        return None

    @staticmethod
    def record_prep(
        record_dict: dict,
        mdf: MDF,
        subgraph_col: str | None = None,
        id_field: str | None = None,
        delimiter: str = ";",
    ) -> dict:
        """
        Prepares a record dictionary for LOCAL record validation by removing certain keys and transform str to list for list type properties if needed
            - Keys that need to be removed: "type",  "guid" and linkage keys which contain "."
                - id_field, such as "guid", is removed because usually it is not part of the data model
            - Keys with empty string or made of only whitespace will be removed
            - Keys with list type will be converted to list if the value is not empty string or made of only whitespace
        The file does not need to have id_field column, but if it does, we want to remove it before validation since it's not part of the model definition.

        Args:
            record_dict (dict): The original record dictionary.
            mdf (MDF): The MDF object containing the model definition.
            subgraph_col (str | None): The name of the column that indicates subgraph information. default is "subgraph".
            id_field (str | None): The name of the id field, default is "guid"
            delimiter (str): The delimiter used for list properties in the record dictionary. default is ";"

        Returns:
            dict: The prepared record dictionary.
        """
        record_type = record_dict["type"]

        # remove keys that are irrelavant for validation, including type, guid/id, and linkage related keys which contain "."
        remove_key_list = [
            key
            for key in record_dict.keys()
            if "." in key or key in ["type", id_field, subgraph_col]
        ]
        for key in remove_key_list:
            record_dict.pop(key)

        # remove key if value is empty or made of only whitespace
        key_to_remove = []
        for key in record_dict.keys():
            if record_dict[key] is None:  # this only happens when a short row is read
                key_to_remove.append(key)
            elif (
                record_dict[key].strip() == ""
            ):  # when there is only a placeholder or string made of whitespace
                key_to_remove.append(key)
            else:
                pass
        for key in key_to_remove:
            record_dict.pop(key)

        # convert str to list if property type is list
        # the record_dict should only contain value that is not empty string
        for key in record_dict.keys():
            # there is a chance that a property is not defined in the model
            if key not in mdf.model.nodes[record_type].props:
                continue
            # if the key_prop is defined in the model, check if it's a list.
            else:
                key_prop = mdf.model.nodes[record_type].props[key]
                key_prop_type = key_prop.value_domain
                if key_prop_type == "list":
                    key_value = record_dict[key]
                    key_value_list = [
                        item.strip() for item in key_value.split(delimiter)
                    ]
                    record_dict[key] = key_value_list
                else:
                    pass

        # convert str value to number/float if the property type is number /integer
        for key in record_dict.keys():
            # no need to check if the record_dict[key] is empty string because it should have been removed.
            if key not in mdf.model.nodes[record_type].props:
                continue
            key_prop = mdf.model.nodes[record_type].props[key]
            key_prop_type = key_prop.value_domain
            if key_prop_type in ["number", "integer"]:
                record_dict[key] = Validator.to_number(record_dict[key])
            elif key_prop_type == "list" and key_prop.item_domain in [
                "number",
                "integer",
            ]:
                # the record_dict[key] is already converted to list above
                record_dict[key] = [
                    Validator.to_number(item) for item in record_dict[key]
                ]
            else:
                pass
        return record_dict

    @staticmethod
    def find_tsv_files(folder_path: str, recursive: bool = True) -> list[str]:
        """
        Finds all TSV files in a specified folder.

        Args:
            folder_path: Path to the folder to search for TSV files.
            recursive: Whether to search for TSV files recursively in subfolders. Default is True.
        Returns:
            list[str]: A list of paths to TSV files found in the folder.
        """
        path = Path(folder_path)

        if not path.is_dir():
            raise NotADirectoryError(f"{folder_path} is not a valid directory")

        pattern = "**/*.tsv" if recursive else "*.tsv"
        return list(path.glob(pattern))

    @staticmethod
    def create_subgraph_dict(file_folder_path: str | Path) -> dict[str, list[str]]:
        """
        Creates a subgraph dictionary from TSV files in a specified folder.
        This function only looks at all the files under file_folder_path, and checks for value under "subgrah" column to determine which subgraph the file belongs to.
        The return of this function will help validation of files from different subgraphs, for example, when we have several sets of files for multiple studies

        Args:
            file_folder_path: Path to the folder containing TSV files.
        Returns:
            dict[str, list[str]]: A dictionary indicates which file belongs to which study/program rooted subgraph
        """
        subgraph_dict = {}
        tsv_files = Validator.find_tsv_files(str(file_folder_path), recursive=True)

        for tsv_file in tsv_files:
            tsv_file_path = str(tsv_file)
            encoding = Validator.check_encoding(tsv_file_path)
            with open(
                tsv_file_path, mode="r", encoding=encoding, newline=""
            ) as file_obj:
                reader = csv.DictReader(file_obj, delimiter="\t")
                first_record = next(reader, None)

            if first_record is None:
                print(f"{tsv_file_path} is empty, skipping...")
                continue

            try:
                subgraph_value = first_record.get("subgraph")
                if subgraph_value is None or subgraph_value.strip() == "":
                    raise KeyError(
                        f"{tsv_file_path} does not have a subgraph value, skipping..."
                    )
            except Exception as e:
                raise ValueError(f"Error processing {tsv_file_path}: {e}") from e

            if subgraph_value not in subgraph_dict:
                subgraph_dict[subgraph_value] = [tsv_file_path]
            else:
                subgraph_dict[subgraph_value].append(tsv_file_path)
        return subgraph_dict

    @staticmethod
    def add_subgrapgh_value_to_tsv(
        file_path: str | Path, subgraph_vlaue: str, output_file_path: str
    ) -> str:
        """
        Adds a subgraph value to the "subgraph" column in the file. This is for the purpose of determining which subgraph the file belongs to when loading to graph db.
        This only applied to tsv file type

        Args:
            file_path: Path to the TSV file.
            subgraph_vlaue: The value to be added to the record dict for subgraph key
        Returns:
            str: path of the new file with subgraoph value added
        """
        encoding = Validator.check_encoding(str(file_path))
        try:
            file_df = pd.read_csv(
                str(file_path),
                sep="\t",
                encoding=encoding,
                quotechar='"',
                doublequote=True,
                escapechar="\\",  # add escape char to handle special characters
                keep_default_na=False,
                na_values=[""],  # treat empty strings as NaN
                dtype=str,  # read columns as str. This is to avoid infer columns full of numbers as float64
            )
            file_df["subgraph"] = subgraph_vlaue
            file_df.to_csv(output_file_path, sep="\t", index=False, na_rep="")
            return output_file_path
        except Exception as e:
            print(f"Error reading {str(file_path)}: {e}")
            raise e

    @staticmethod
    def file_type_read(file_path_list: list[str | Path]) -> dict[str, list[str]]:
        """
        Reads a list of file paths and categorizes them by their file type based on the "type" column in the TSV files.
        Example return:
        {
            "participant": ["path/to/participant.tsv"],
            "diagnosis": ["path/to/diagnosis.tsv", "path/to/diagnosis_2.tsv"],
            ...
        }
        In most cases, there would be only one file for each type, but it is possible to have multiple files in the same type.

        Args:
            file_path_list: A list of file paths to read and categorize.
        Returns:
            dict[str, list[str]]: A dictionary where the keys are file types and the values are lists of file paths corresponding to each type.
        """
        type_file_dict = {}
        for file_path in file_path_list:
            encoding = Validator.check_encoding(str(file_path))
            try:
                file_df = pd.read_csv(
                    str(file_path),
                    sep="\t",
                    encoding=encoding,
                    quotechar='"',
                    doublequote=True,
                    escapechar="\\",  # add escape char to handle special characters
                    keep_default_na=False,
                    na_values=[""],  # treat empty strings as NaN
                )
                if "type" not in file_df.columns:
                    raise KeyError(
                        f"No 'type' column found in {str(file_path)}, unable to determine file type for categorization"
                    )
                elif file_df["type"].nunique() > 1:
                    raise ValueError(
                        f"Multiple types found in 'type' column of {str(file_path)}, unable to determine file type for categorization. Types found: {','.join(file_df['type'].unique().tolist())}"
                    )
                elif file_df["type"].isna().any():
                    raise ValueError(
                        f"'type' column in {str(file_path)} contains NA value, unable to determine file type for categorization, please add any missing type value"
                    )
                else:
                    file_type = file_df["type"].iloc[0]
                    if file_type not in type_file_dict:
                        type_file_dict[file_type] = [str(file_path)]
                    else:
                        type_file_dict[file_type].append(str(file_path))
            except Exception as e:
                print(f"Error processing {str(file_path)}: {e}")
                raise e

        return type_file_dict

    @staticmethod
    def read_record_by_row_in_tsv(
        tsv_file_path: str,
        row_number: int,
        mdf_instance: MDFReader,
        keep_id_field: bool,
        id_field: str = "guid",
        delimiter: str = ";",
    ) -> dict[str, str]:
        """
        ########################
        # FOR VALIDATION IN DB #
        ########################
        Reads a specific row from a TSV file and returns it as a dictionary keyed by column name.
        Note: this function allows you to decide wether to keep id_field in the record

        Args:
            tsv_file_path: Path to the TSV file.
            row_number: The row number to read (1-based index).

        Returns:
            dict[str, str]: A dictionary representing the specified row, keyed by column name.
        """
        encoding = Validator.check_encoding(tsv_file_path)
        try:
            with open(
                tsv_file_path, mode="r", encoding=encoding, newline=""
            ) as tsv_file:
                reader = csv.DictReader(tsv_file, delimiter="\t")
                # DictReader already consumes the header, so its first row is row 2.
                # index into the data rows: row_number 2 -> data index 0
                # if the row_number is less than 2 or past the end of the file, it will return None
                row = next(islice(reader, row_number - 2, row_number - 1), None)
                if row is not None:
                    # capture id_field value before record_prep mutates the dict
                    id_field_value = row.get(id_field, None)
                    # prep the record
                    row_record = Validator.record_prep(
                        row,
                        mdf=mdf_instance,
                        subgraph_col=None,
                        id_field=id_field,
                        delimiter=delimiter,
                    )
                    if keep_id_field:
                        row_record[id_field] = id_field_value
                    return row_record
                else:
                    return {}
        except Exception as e:
            print(f"Error reading row {row_number} from {tsv_file_path}: {e}")
            raise e

    @staticmethod
    def read_full_records_in_tsv(
        tsv_file_path: str,
        mdf_instance: MDFReader,
        keep_id_field: bool,
        id_field: str = "guid",
        delimiter: str = ";",
    ) -> Iterator[dict[str, str]]:
        """
        ########################
        # FOR VALIDATION IN DB #
        ########################
        Streams every data row of a TSV file as a prepared record, in file order, using a
        single pass over the file. This is the iterator counterpart of
        read_record_by_row_in_tsv: use it inside a sequential row loop (e.g. zipped with the
        id/rels streams) instead of calling read_record_by_row_in_tsv per row, which would
        re-scan the file each time (quadratic on large files).

        Note: like read_record_by_row_in_tsv, this lets you decide whether to keep id_field
        in the record.

        Args:
            tsv_file_path: Path to the TSV file.
            mdf_instance: MDFReader instance passed to record_prep.
            keep_id_field: Whether to keep the id_field value in each returned record.
            id_field: The name of the id field, default "guid".
            delimiter: The delimiter used within multi-value cells, default ";".

        Yields:
            dict[str, str]: The prepared record for each data row, in file order (first
                yielded record corresponds to data row 2, since the header is row 1).
        """
        encoding = Validator.check_encoding(tsv_file_path)
        try:
            with open(tsv_file_path, mode="r", encoding=encoding, newline="") as tsv_file:
                reader = csv.DictReader(tsv_file, delimiter="\t")
                for row in reader:
                    # capture id_field value before record_prep mutates the dict
                    id_field_value = row.get(id_field, None)
                    # prep the record
                    row_record = Validator.record_prep(
                        row,
                        mdf=mdf_instance,
                        subgraph_col=None,
                        id_field=id_field,
                        delimiter=delimiter,
                    )
                    if keep_id_field:
                        row_record[id_field] = id_field_value
                    yield row_record
        except Exception as e:
            print(f"Error reading records from {tsv_file_path}: {e}")
            raise

    @classmethod
    def read_tsv_records(
        cls,
        tsv_file_path: str,
        mdf: MDF,
        subgraph_col: str | None = None,
        id_field: str | None = None,
        delimiter: str = ";",
    ) -> Iterator[tuple[str, dict[str, str]]]:
        """
        ########################
        # FOR LOCAL VALIDATION #
        ########################
        Reads a TSV file and yields each row as a dictionary keyed by column name.
        The file does not need to have subgraph column, but if it does, we want to remove it before validation since it's not part of the model definition.

        Args:
            tsv_file_path: Path to the TSV file.
            mdf: MDF object containing the model definition, used for record preparation.
            subgraph_col: The name of the column that indicates subgraph information, default is "subgraph"
            id_field: The name of the id field, default is "guid"
            delimiter: The delimiter used for list properties in the record dictionary. default is ";"

        Yields:
            dict[str, str]: One record per row.
        """
        encoding = cls.check_encoding(tsv_file_path)
        with open(tsv_file_path, mode="r", encoding=encoding, newline="") as tsv_file:
            reader = csv.DictReader(tsv_file, delimiter="\t")
            for row in reader:
                row_dict = dict(row)
                row_type = row_dict["type"]
                row_dict = cls.record_prep(
                    row_dict,
                    mdf,
                    subgraph_col=subgraph_col,
                    id_field=id_field,
                    delimiter=delimiter,
                )
                yield row_type, row_dict

    @classmethod
    def read_tsv_records_id(
        cls, tsv_file_path: str, id_field: str = "guid"
    ) -> Iterator[tuple[str, dict[str, str]]]:
        """
        ########################
        # FOR VALIDATION IN DB #
        ########################
        This function reads a TSV file and yields type and id_field value for each row.
        Only use this function after guid/uuid has been generated.
        This will be fused for validation in the db to check if the record exist in the db based on id_field value
        An example of read_tsv_records_id output:
        "participant", {"guid": "395aa6ed-b912-483f-bf20-16e0313b21ae"}

        Args:
            tsv_file_path (str): filepath of a tsv file
            id_field (str, optional): global unique identifier field which is used to check if the record exists in the database. Defaults to "guid".

        Yields:
            Iterator[tuple[str, dict[str, str]]]: type of the record, the value of the id_field
        """
        encoding = cls.check_encoding(tsv_file_path)
        with open(tsv_file_path, mode="r", encoding=encoding, newline="") as tsv_file:
            reader = csv.DictReader(tsv_file, delimiter="\t")
            for row in reader:
                row_dict = dict(row)
                row_type = row_dict.get("type", None)  # in case there is no type
                row_id_value = row_dict.get(
                    id_field, None
                )  # in case there is no id_field
                yield row_type, {id_field: row_id_value}

    @classmethod
    def read_tsv_rels_id(
        cls,
        tsv_file_path: str,
        id_field: str = "guid",
        delimiter: str = ";",
    ) -> Iterator[tuple[str, dict[str, str]]]:
        """
        ########################
        # FOR VALIDATION IN DB #
        ########################
        A generator function that reads a TSV file containing relationship data and yields each row as a list of relationship dictionaries. Each relationship dictionary contains the source label, source ID property, source ID value, destination label, destination ID property, and destination ID value.
        Only use this function after guid/uuid has been generated.
        An example of read_tsv_rels output:
        [
            {
                "src_label": "sample",
                "src_id_prop": "guid",
                "src_id_value": "891e7e1a-9c25-4d25-a4b8-1517233f3f8f",
                "dst_label": "participant",
                "dst_id_prop": "guid",
                "dst_id_value": "395aa6ed-b912-483f-bf20-16e0313b21ae",
            },
            {
                "src_label": "sample",
                "src_id_prop": "guid",
                "src_id_value": "9164e720-c995-446b-9eee-d6161c0d83fa",
                "dst_label": "study",
                "dst_id_prop": "guid",
                "dst_id_value": "0c0a9b5f-0ad0-474d-b34e-ec0a24497e7b",
            },
            ...
        ]

        Args:
            tsv_file_path: Path to the TSV file.
            id_field: The name of the id field, default is "guid"
            delimiter: The delimiter used for list properties in the record dictionary. default is ";"

        Yields:
            dict[str, str]: One record per row.
        """
        encoding = cls.check_encoding(tsv_file_path)
        with open(tsv_file_path, mode="r", encoding=encoding, newline="") as tsv_file:
            reader = csv.DictReader(tsv_file, delimiter="\t")
            for row in reader:
                row_rel_list = []
                row_dict = dict(row)
                row_id_value = row_dict.get(id_field)
                row_type = row_dict.get("type", None)  # it must contain type column
                for key in row_dict.keys():
                    if f".{id_field}" in key:  # this is a relationship column
                        parent_label = key.split(".")[0]
                        if (
                            row_dict[key].strip().strip(delimiter) != ""
                        ):  # if the value is not empty or made of only whitespace
                            parent_id_value = [
                                i.strip()
                                for i in row_dict[key]
                                .strip()
                                .strip(delimiter)
                                .split(delimiter)
                            ]  # strip after split
                            for parent_id in parent_id_value:
                                row_rel_list.append(
                                    {
                                        "src_label": row_type,
                                        "src_id_prop": id_field,
                                        "src_id_value": row_id_value,
                                        "dst_label": parent_label,
                                        "dst_id_prop": id_field,
                                        "dst_id_value": parent_id,
                                    }
                                )
                        else:
                            pass
                    else:
                        pass
                yield row_rel_list

    def validate_records(
        self, node_name: str, list_of_records: List[dict]
    ) -> tuple[bool, dict[str, Any]]:
        """
        ########################
        # FOR LOCAL VALIDATION #
        ########################
        Validates node level data entries (multiple), such as participant records, from a node in dict format using the MDFDataValidator.

        Args:
            node_name: The name of the node to validate.
            list_of_records: A list of dictionaries representing the records to validate.

        Returns:
            bool: True if the data is valid, False if at least one record is invalid.
            dict[str, Any]: A dictionary of validation warning/error messages, if any.
        """

        is_valid = False
        warning_error_messages = {}
        validate_result = self.record_validator.validate(
            handle_name=node_name, data=list_of_records
        )
        if validate_result:
            is_valid = True
        else:
            warning_error_messages["warnings"] = (
                self.record_validator._validation_warnings
            )
            warning_error_messages["errors"] = self.record_validator._validation_errors
        return is_valid, warning_error_messages

    def validate_one_record(
        self, node_name: str, record: dict
    ) -> tuple[bool, dict[str, Any]]:
        """
        ########################
        # FOR LOCAL VALIDATION #
        ########################
        Validates a single node level data entry, such as a participant record, from a node in dict format using the MDFDataValidator.

        Args:
            node_name: The name of the node to validate.
            record: A dictionary representing the record to validate.

        Returns:
            bool: True if the data is valid, False otherwise.
            dict[str, Any]: A dictionary of validation warning/error messages, if any.
        """

        is_valid = False
        warning_error_messages = {}
        validate_result = self.record_validator.validate(
            handle_name=node_name, data=[record]
        )
        if validate_result:
            is_valid = True
            # if is_valid is true, self.record_validaotr._validation_warnings is None, and self.record_validaotr._validation_errors is None
        else:
            # clean up enum error or warning messages to make them short
            # is_valid = False
            # self.record_validator._validation_warnings/_validation_errors is either [] or a dict with key 0 (because we are only testing one record)
            if self.record_validator._validation_warnings is None:
                warning_error_messages["warnings"] = []
            elif (
                len(self.record_validator._validation_warnings) > 0
            ):  # if self.record_validator._validation_warnings is not None
                short_warnings = self._validate_records_messages_cleanup(
                    messages=self.record_validator._validation_warnings[0]
                )
                warning_error_messages["warnings"] = short_warnings
            else:
                warning_error_messages["warnings"] = []

            if (
                len(self.record_validator._validation_errors) > 0
            ):  # if self.record_validator._validation_errors is not None
                short_errors = self._validate_records_messages_cleanup(
                    messages=self.record_validator._validation_errors[0]
                )
                warning_error_messages["errors"] = short_errors
            else:
                warning_error_messages["errors"] = []
        return is_valid, warning_error_messages

    def _validate_records_messages_cleanup(
        self, messages: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """A helper function that cleans up validation messages from validate_one_record
        Enum warning or error generates redundant messages for each record, this function is to clean up the messages to only keep simplified messages for each enum validation failure.

        Args:
            messages (list[dict[str, Any]]): a list of warnings or errors

        Returns:
            list[dict[str, Any]]:
        """
        cleaned_messages = []
        for i in messages:
            if i["type"] == "enum":
                i_property = i["loc"][
                    0
                ]  # only expect one property for each enum error or warning
                if i["level"] == "warning":
                    i["msg"] = (
                        f"Input not found in the permissible value list for {i_property}, but a free string is Allowed. Please refer to the data model for the list of allowed enum values"
                    )
                elif i["level"] == "error":
                    i["msg"] = (
                        f"Input not found in the permissible value list for {i_property}, and a free string is Not Allowed. Please refer to the data model for the list of allowed enum values"
                    )
                else:
                    raise ValueError(
                        f"Unexpected message level {i['level']} found in validation messages, expected 'warning' or 'error'"
                    )
                # remove "ctx" in i
                if "ctx" in i:
                    i.pop("ctx")
                cleaned_messages.append(i)
            else:  # no change to other non-enum message
                cleaned_messages.append(i)
        return cleaned_messages

    def validate_tsv_records(
        self,
        file_path: str,
        subgraph_col: str | None = None,
        id_field: str | None = None,
        delimiter: str = ";",
    ) -> list[dict, Any]:
        """
        ########################
        # FOR LOCAL VALIDATION #
        ########################
        Validates records from a TSV file and returns the validation results.
        although the validator_record can take a dict or a list of dict, we only validate one record at a time here.

        Args:
            file_path: Path to the TSV file.
            subgraph_col: The name of the column that indicates subgraph information, default is "subgraph"
            id_field: The name of the id field, default is "guid"
            delimiter: The delimiter used for list properties in the record dictionary. default is ";"
        Returns:
            dict[str, Any]: A dictionary containing validation results, including validity status and any warning/error messages.
        """

        validation_results = []
        row_num = 2  # the record starts frm the second row in the file
        for node_name, record in self.read_tsv_records(
            file_path,
            self.mdf,
            subgraph_col=subgraph_col,
            id_field=id_field,
            delimiter=delimiter,
        ):
            if (
                node_name == "" and record == {}
            ):  # this happens when there is an empty line
                validation_results.append(
                    {
                        "row": row_num,
                        "is_valid": False,
                        "messages": {
                            "warnings": [],
                            "errors": [
                                {
                                    "level": "error",
                                    "type": "missing",
                                    "loc": None,
                                    "msg": "This line is empty",
                                    "input": None,
                                    "url": "https://docs.pydantic.dev/2.12/errors/validation_errors/#missing",
                                }
                            ],
                        },
                    },
                )
            elif (
                node_name == "" and record != {}
            ):  # this happens when the "type" column is empty but other columns have value
                validation_results.append(
                    {
                        "row": row_num,
                        "is_valid": False,
                        "messages": {
                            "warnings": [],
                            "errors": [
                                {
                                    "level": "error",
                                    "type": "missing",
                                    "loc": ["type"],
                                    "msg": "Missing data type information in 'type' column, unable to validate this record",
                                    "input": None,
                                    "url": "https://docs.pydantic.dev/2.12/errors/validation_errors/#missing",
                                }
                            ],
                        },
                    },
                )
            else:  # use the validator_record to validate
                is_valid, messages = self.validate_one_record(node_name, record)

                if not is_valid:
                    validation_results.append(
                        {"row": row_num, "is_valid": is_valid, "messages": messages}
                    )
                else:
                    pass
            row_num += 1
        # quick check of invalid records found
        # if len(validation_results) == 0:
        #    print(f"all records of {os.path.basename(file_path)} are valid!")
        # else:
        #    print(f"Invalid recors found in {os.path.basename(file_path)}: {len(validation_results)}")
        return validation_results

    @staticmethod
    def if_rel_valid(child_type: str, mdf: MDF, rel_to_test: str) -> bool:
        """
        A helper function to test if a relationship value is valid based on the model definition.
        "rel_to_test" is expected to be in the format of <parent_node>.<parent_node_key_prop>, for example, participant.participant_id.
        This is used for validating relationship column value in the tsv file, which is expected to be in the format of <parent_node>.<parent_node_key_prop>, for example, participant.participant_id
        The validation test two part,
            - if the parent node is the real parent node
            - if the parent node key prop is the real key prop for parent node.

        Args:
            child_type: The type of the child node that has the relationship column to be tested
            mdf: MDF object containing the model definition, used for record preparation.
            rel_to_test: The relationship column value to be tested, expected to be in the format of <parent_node>.<parent_node_key_prop>, for example, participant.participant_id
        Returns:
            bool: True if the relationship value is valid based on the model definition, False otherwise.
        """
        if "." not in rel_to_test:
            return False
        else:
            test_parent_node, test_parent_node_key_prop = rel_to_test.split(".")
            edges_list = mdf.model.edges_by_src(mdf.model.nodes[child_type])
            parent_node_list = [e.triplet[2] for e in edges_list]
            if test_parent_node not in parent_node_list:
                return False
            else:
                test_parent_node_key_prop_mdf = (
                    mdf.model.nodes[test_parent_node].get_key_prop().handle
                )
                if test_parent_node_key_prop != test_parent_node_key_prop_mdf:
                    return False
                else:
                    return True

    @staticmethod
    def get_rel_multiplicity(node_type: str, parent_node_type: str, mdf: MDF) -> str:
        """
        A helper function to get the relationship multiplicity based on the model definition.
        This is used for validating relationship column value in the tsv file. If "many_to_many" or "one_to_many" is found, the relationship value will be parsed by a deilmiter if a delimiter is present.

        Args:
            node_type: The type of the child node that has the relationship column to be tested
            parent_node_type: The type of the parent node in the relationship
            mdf: MDF object containing the model definition, used for record preparation.
        Returns:
            str: The relationship multiplicity, which can be
        """
        rel_multi = None
        try:
            edges_list = mdf.model.edges_by_src(mdf.model.nodes[node_type])
            for edge in edges_list:
                if edge.triplet[2] == parent_node_type:
                    rel_multi = edge.multiplicity
                    break
            else:
                pass
        except KeyError as e:
            raise KeyError(
                f"Error when getting edges with {node_type} as source: {e}"
            ) from e
        except Exception as e:
            raise e
        return rel_multi

    def validate_tsv_rels(
        self, file_path_list: list[str | Path], rel_delimiter: str = ";"
    ) -> dict[str, Any]:
        """
        ########################
        # FOR LOCAL VALIDATION #
        ########################
        Validates relationship records from a set of TSV files in a specified folder and returns the validation results for crosslinks.
        This function assumes the provided file list only contains files from the SAME SUBGRAPH. For instance, submission file for phs002790 study.

        This function can ONLY be used before the uuid5 generaiton. The relationship column name shoud be <parent_node>.<parent_node_key_prop>.
        For instance, participant.participant_id
        This function does not use generator to read relationship records.

        Args:
            mdf: MDF object containing the model definition, used for record preparation.
            file_folder_path: Path to the folder containing TSV files with relationship records.
        Returns:
            dict[str, Any]: A dictionary containing validation results for relationship records, including validity status and any warning/error messages.
        """
        # validation results only shows invalid records. No records for valid records
        validation_results = {}
        # read the type of all the files
        # if there is any issue with type reading, the error will be raised here
        type_file_dict = self.file_type_read(file_path_list)

        mdf = self.mdf

        for file in file_path_list:
            encoding = Validator.check_encoding(str(file))
            try:
                file_df = pd.read_csv(
                    str(file),
                    sep="\t",
                    encoding=encoding,
                    quotechar='"',
                    doublequote=True,
                    escapechar="\\",  # add escape char to handle special characters
                    keep_default_na=False,
                    na_values=[""],  # treat empty strings as NaN
                    dtype=str,  # enforce all the columns read as str to avoid miss interpretion of rel columns as float type
                    # this change won't affect record valdiation, as this method is only reading relationship cols
                )
                file_type = file_df["type"].iloc[0]
                rel_cols = [col for col in file_df.columns if "." in col]

                # check if the file has the right format to check relationships
                # the validation would raise ValueError if the file has relationship col for root node type, or no relationship for non-root node type
                edges_list_by_model = [
                    e.triplet
                    for e in self.model.edges_by_src(self.model.nodes[file_type])
                ]
                if len(edges_list_by_model) == 0 and len(rel_cols) > 0:
                    # this shouldn't happen if the file passes tsv format checking. The format checking will report error of invalid rel col
                    raise ValueError(
                        f"Invalid relationship column {rel_col} found in file {str(file)}. Either the parent node is not found in MDF or the parent node key prop isn't correct"
                    )
                elif len(edges_list_by_model) > 0 and len(rel_cols) == 0:
                    # this shouldn't happen if the file passes tsv format checking
                    raise ValueError(
                        f"Missing relationship column for non-root node type {file_type} in the file. At least one edges is expected: {','.join(map(str, edges_list_by_model))}"
                    )
                elif len(edges_list_by_model) == 0 and len(rel_cols) == 0:
                    # this is the case for root node which doesn't have relationship column, just pass the validation
                    pass
                else:  # this is the case for non-root node, validate the relationship column value
                    # this condition len(edges_list_by_model) > 0 and len(rel_cols) > 0 is expected to be true for non-root node
                    pass

                # If file_type is not root node, check if any row/entry has at least one linkage
                # we want to avoid any floating data node in the graph
                if len(rel_cols) > 0:  # only check when rel_col is not empty
                    index_missing_linkage = file_df[
                        file_df[rel_cols].isna().all(axis=1)
                    ].index.tolist()
                    if (
                        len(index_missing_linkage) > 0
                    ):  # only report if missing rel is found
                        row_missing_linkage = [
                            i + 2 for i in index_missing_linkage
                        ]  # add 2 to get the actual row number in the file since the index starts from 0 and the record starts from the second row in the file
                        for row in row_missing_linkage:
                            if str(file) not in validation_results:
                                validation_results[str(file)] = []
                            validation_results[str(file)].append(
                                {
                                    "row": row,
                                    "edge_column": "N/A",
                                    "invalid_value": "N/A",
                                    "edge_src": file_type,
                                    "edge_dst": "N/A",
                                    "message": f"Missing relationship value for non-root node type {file_type} in the file. At least one edges is expected: {','.join(map(str, edges_list_by_model))}",
                                }
                            )
                    else:
                        pass

                for rel_col in rel_cols:
                    # check if the relationship is valid based on the model definition
                    if not Validator.if_rel_valid(file_type, mdf, rel_col):
                        raise ValueError(
                            f"Invalid relationship column {rel_col} found in file {str(file)}. Either the parent node is not found in MDF or the parent node key prop isn't correct"
                        )
                    else:
                        pass

                    # if the entire column is empty, skip this column
                    if file_df[rel_col].isna().all():
                        continue
                    else:
                        # not all values in the relationship column are empty
                        pass

                    rel_multi = Validator.get_rel_multiplicity(
                        file_type, rel_col.split(".")[0], mdf
                    )
                    rel_col_parent, rel_col_parent_key_prop = rel_col.split(".")
                    parent_files = type_file_dict.get(rel_col_parent)
                    # there is a chance that parent_files return None. Add an error message if this happens for rel_col
                    # At this point, we already that rel_col is not empty
                    if parent_files is None:
                        if str(file) not in validation_results:
                            validation_results[str(file)] = []
                        # add error message for missing parent file for this relationship column
                        validation_results[str(file)].append(
                            {
                                "row": "N/A",
                                "edge_column": rel_col,
                                "invalid_value": "N/A",
                                "edge_src": file_type,
                                "edge_dst": rel_col_parent,
                                "message": f"Failed to find {rel_col_parent} type file for NONEMPTY relationship column '{rel_col}' in the provided file list: {[str(i) for i in file_path_list]}",
                            }
                        )
                        continue
                    parent_key_values = []
                    for parent_file in parent_files:
                        parent_encoding = Validator.check_encoding(str(parent_file))
                        parent_file_df = pd.read_csv(
                            str(parent_file),
                            sep="\t",
                            encoding=parent_encoding,
                            quotechar='"',
                            doublequote=True,
                            escapechar="\\",  # add escape char to handle special characters
                            keep_default_na=False,
                            na_values=[""],  # treat empty strings as NaN
                            dtype=str,  # Also only reads df as str type as we only need information of key prop col
                        )
                        parent_key_values += (
                            parent_file_df[rel_col_parent_key_prop]
                            .dropna()
                            .astype(str)
                            .tolist()
                        )
                    # only keep unique values in the parent_key_values
                    parent_key_values = list(set(parent_key_values))

                    rel_col_values = file_df[rel_col]
                    for i in rel_col_values.index:
                        # row number is i+2 since the index starts from 0 and the record starts from the second row in the file
                        if (
                            pd.isna(rel_col_values[i])
                            or str(rel_col_values[i]).strip() == ""
                        ):
                            continue
                        else:
                            # only parse rel_col_values[i] when the relationship multiplicity is many_to_many or one_to_many, otherwise treat the whole value as one value
                            if rel_multi in ["many_to_many", "one_to_many"]:
                                i_value_list = [
                                    item.strip()
                                    for item in str(rel_col_values[i]).split(
                                        rel_delimiter
                                    )
                                ]
                                for item in i_value_list:
                                    if item not in parent_key_values:
                                        # in case that key wasn't added to the validation result dict, initialize it with empty list
                                        if str(file) not in validation_results:
                                            validation_results[str(file)] = []
                                        validation_results[str(file)].append(
                                            {
                                                "row": i
                                                + 2,  # add 2 to get the actual row number in the file since the index starts from 0 and the record starts from the second row in the file
                                                "edge_column": rel_col,
                                                "invalid_value": item,
                                                "edge_src": file_type,
                                                "edge_dst": rel_col_parent,
                                                "message": f"Failed to find '{item}' in '{rel_col_parent}' file at column '{rel_col_parent_key_prop}': {', '.join(parent_files)}",
                                            }
                                        )
                                    else:
                                        pass
                            else:
                                if (
                                    str(rel_col_values[i]).strip()
                                    not in parent_key_values
                                ):
                                    if str(file) not in validation_results:
                                        validation_results[str(file)] = []
                                    validation_results[str(file)].append(
                                        {
                                            "row": i
                                            + 2,  # add 2 to get the actual row number in the file since the index starts from 0 and the record starts from the second row in the file
                                            "edge_column": rel_col,
                                            "invalid_value": str(rel_col_values[i]),
                                            "edge_src": file_type,
                                            "edge_dst": rel_col_parent,
                                            "message": f"Failed to find '{str(rel_col_values[i])}' in '{rel_col_parent}' file at column '{rel_col_parent_key_prop}': {', '.join(parent_files)}",
                                        }
                                    )
                                else:
                                    pass
            except Exception as e:
                print(f"Error processing {str(file)}: {e}")
                raise e
        return validation_results

    @staticmethod
    def read_tsv_key_prop_values(
        file_path: str | Path, key_prop: str, chunk_size: int = 5000
    ) -> Iterator[str]:
        """Read the key prop values of a tsv file

        Args:
            file_path (str | Path): file path or filepath object
            key_prop (str): key property name

        Returns:
            Iterator[str]: An iterator over the key property values in the TSV file.
        """
        encoding = Validator.check_encoding(str(file_path))
        try:
            for chunk in pd.read_csv(
                str(file_path),
                sep="\t",
                encoding=encoding,
                quotechar='"',
                doublequote=True,
                escapechar="\\",  # add escape char to handle special characters
                keep_default_na=False,
                na_values=[""],  # treat empty strings as NaN
                dtype=str,  # read columns as str. This is to avoid infer columns full of numbers as float64
                chunksize=chunk_size,
            ):
                for index, val in chunk[key_prop].items():
                    yield index, ("" if pd.isna(val) else val.strip())
        except Exception as e:
            print(f"Error reading {str(file_path)}: {e}")
            raise e

    @staticmethod
    def identify_duplicated_values(
        value_list: list[dict[str, Any]], key: str
    ) -> list[dict[str, Any]]:
        """
        Identify duplicated values in a list of dictionaries based on a specified key.
        value_list is expected to be a list of dictionaries:
        [
        {
            "type": "participant",
            "file_path": "path/to/participant.tsv",
            "key_prop": "participant_id",
            "key_prop_value": "participant_001",
            "row": 2,
        },
        ...
        ]

        Args:
            value_list: A list of dictionaries containing the values to be checked for duplicates.
            key: The key in the dictionaries to check for duplicate values.
        Returns:
            list[dict[str, Any]]: A list of dictionaries containing the duplicated records
        """
        # turn value_list in to a dataframe for easier manipulation
        value_df = pd.DataFrame(value_list)
        duplicated_df = value_df[value_df.duplicated(subset=[key], keep=False)].copy()
        duplicated_df.sort_values(
            by=["type", "key_prop", "key_prop_value", "row"], inplace=True
        )
        duplicated_values = duplicated_df.to_dict(orient="records")
        return duplicated_values

    def validate_tsv_uniq_entry(
        self, file_path_list: list[str | Path]
    ) -> list[dict[str, Any]]:
        """
        ########################
        # FOR LOCAL VALIDATION #
        ########################
        Validates the uniqueness of data entry within a list of tsv files for a subgraph submission.
        We ASSUME the provided files are from the SAME subgraph. In some cases, it can be from the same study, but in other cases, it can be from the same program with multiple studies under it.
        Because two entries of [same key property value] in the [same type] under the [same subgraph] will share the same UUID

        The function will first look at the type of each files, and then look for duplicated entry (key property) within the same type files.
        For example, if a participant_id (key prop for participant node) value appear in two participant type files, it will be considered as duplicated entry.
        NOTE: It is okay to have identical prop key value of same type of data node under different rooted subgraph.
        For example, different studies can share same sample_id as long as they are from different studies, which means the guid/uuid would be different for these data nodes

        Args:
            file_path_list: A list of paths to TSV files to be validated for unique entries in the id field column.
        Returns:
            list[dict[str, Any]]: A list of dictionaries containing validation results for duplicated entries in the id field column, including validity status and any warning/error messages.
        """
        validation_results = []
        data_start_offset = 2  # data line starts at line 2

        type_file_dict = self.file_type_read(file_path_list)
        for type in type_file_dict:
            type_file_list = type_file_dict[type]
            # get key prop
            key_prop = self.model.nodes[type].get_key_prop().handle
            key_prop_list = []
            for file in type_file_list:
                for index, key_prop_value in self.read_tsv_key_prop_values(
                    file, key_prop
                ):
                    key_prop_list.append(
                        {
                            "type": type,
                            "file_path": str(file),
                            "key_prop": key_prop,
                            "key_prop_value": key_prop_value,
                            "row": index + data_start_offset,
                        }
                    )
            duplicated_key_list = self.identify_duplicated_values(
                key_prop_list, "key_prop_value"
            )
            if len(duplicated_key_list) > 0:
                validation_results += duplicated_key_list
            else:
                pass
        return validation_results

    def validate_tsv_format(self, file_path: str | Path) -> list[dict[str, Any]]:
        """
        ########################
        # FOR LOCAL VALIDATION #
        ########################
        Validates the format of a TSV file
        - if type column exist
            - if no, error message and no further validation items
            - if yes, is there any missing value in the "type" column
            - if yes, are there more than one unique value found in the "type" column
        - if so far no error for the file, it indicates the type column is valid.
            - check if the type is a valid in the data model
                - if no, error message and no further validation items
                - if yes, check if all required properties for this type are found in the columns
                - if yes, check if ther are any column that are not defined as properties for the type in the model definition.
                - if yes, check relationship columns are valid
                    - if no relationship column found, check if the type column is the root node. Error message if at least one rel is expected
                    - if relationship column found, check if the relationship column value is valid based on the file type. If not valid, either the parent node is not specified in the model definition, or the parent node key prop is not correct

        Args:
            file_path: Path to the TSV file to be validated for format issues.
        Returns:
            list[dict[str, Any]]: A list of dictionaries containing validation results for TSV format issues, including validity status and any warning/error messages.
        """
        validation_errors = []
        encoding = Validator.check_encoding(str(file_path))
        try:
            file_df = pd.read_csv(
                str(file_path),
                sep="\t",
                encoding=encoding,
                quotechar='"',
                doublequote=True,
                escapechar="\\",  # add escape char to handle special characters
                keep_default_na=False,
                na_values=[""],  # treat empty strings as NaN
            )
            # check if "type" column exist
            if "type" not in file_df.columns:
                validation_errors.append(
                    {
                        "level": "error",
                        "type": "missing_column",
                        "message": "Missing 'type' column in the TSV file, unable to determine file type for validation",
                    }
                )
            else:  # there is type column in the file
                # check if there is any empty row at type column, and report any row missing type value as error
                if (
                    file_df["type"].isna().any()
                    or (file_df["type"].apply(lambda x: str(x).strip() == "")).any()
                ):
                    empty_type_index = file_df[
                        file_df["type"].isna()
                        | (file_df["type"].apply(lambda x: str(x).strip() == ""))
                    ].index.tolist()
                    empty_type_row = [index + 2 for index in empty_type_index]
                    validation_errors.append(
                        {
                            "level": "error",
                            "type": "missing_type_value",
                            "message": "Missing data type information in 'type' column, unable to validate this record",
                            "row": empty_type_row,  # add 2 to get the actual row number in the file since the index starts from 0 and the record starts from the second row in the file
                        }
                    )
                else:
                    pass
                # if "type" column exist, after removing missing or empty value, is there only one unique value found under type column
                type_values = file_df["type"]
                filtered_type_values = type_values[
                    ~type_values.isna()
                    & (type_values.apply(lambda x: str(x).strip() != ""))
                ]
                if filtered_type_values.nunique() > 1:
                    validation_errors.append(
                        {
                            "level": "error",
                            "type": "multiple_type_value",
                            "message": f"Multiple types found in 'type' column, unable to determine file type.",
                            "input": filtered_type_values.unique().tolist(),
                        }
                    )
                else:
                    pass
            # if no error of type column is found, str(file) is not found in validation_errors
            if len(validation_errors) == 0:
                # get the file type
                file_type = file_df["type"].iloc[0]
                if file_type not in self.model.nodes:
                    validation_errors.append(
                        {
                            "level": "error",
                            "type": "invalid_file_type",
                            "message": f"Invalid file type '{file_type}' in 'type' column, not found in the data model definition.",
                        }
                    )
                else:
                    # file_type is valid, we can further check if the rest of the columns are valid based on the model
                    # check if all required properties are found in the columns of the files
                    required_props_for_type = [
                        i
                        for i in self.model.nodes[file_type].props
                        if self.model.nodes[file_type].props[i].is_required is True
                    ]
                    missing_required_props = [
                        col
                        for col in required_props_for_type
                        if col not in file_df.columns
                    ]
                    if len(missing_required_props) > 0:
                        validation_errors.append(
                            {
                                "level": "error",
                                "type": "missing_required_column",
                                "message": f"Missing required column(s) for file type '{file_type}' based on the data model definition: {', '.join(f"'{prop}'" for prop in missing_required_props)}",
                            }
                        )
                    else:
                        pass
                    # check if the rest of columns other than "type" and relationship columns are valid based off model
                    col_to_check = [
                        col
                        for col in file_df.columns
                        if col != "type" and "." not in col
                    ]
                    invalid_cols = [
                        col
                        for col in col_to_check
                        if col not in self.model.nodes[file_type].props
                    ]
                    if len(invalid_cols) > 0:
                        validation_errors.append(
                            {
                                "level": "warning",
                                "type": "invalid_property_column",
                                "message": f"Invalid column(s) found in the file that are not defined as properties for file type '{file_type}' in the data model definition: {', '.join(f"'{col}'" for col in invalid_cols)}",
                            }
                        )
                    else:
                        pass
                    # check relationship columns if valid which contain "."
                    rel_cols = [col for col in file_df.columns if "." in col]
                    # if there is no relationship column, check if the file_type is root node, means there shouldn't be any relationship that src from this type
                    if len(rel_cols) == 0:
                        edges_list = [
                            e.triplet
                            for e in self.model.edges_by_src(
                                self.model.nodes[file_type]
                            )
                        ]
                        if len(edges_list) > 0:
                            edges_dst_list = [e[2] for e in edges_list]
                            validation_errors.append(
                                {
                                    "level": "error",
                                    "type": "missing_relationship_column",
                                    "message": f"Missing relationship column for file type '{file_type}' which is expected to have relationship based on the data model definition. Dst type for {file_type} are: {', '.join(f"'{dst}'" for dst in edges_dst_list)}",
                                }
                            )
                        else:
                            # file_type is a root node which doesn't have any parent node
                            pass
                    else:
                        invalid_rel_cols = [
                            col
                            for col in rel_cols
                            if not self.if_rel_valid(
                                child_type=file_type, mdf=self.mdf, rel_to_test=col
                            )
                        ]
                        if len(invalid_rel_cols) > 0:
                            validation_errors.append(
                                {
                                    "level": "error",
                                    "type": "invalid_relationship_column",
                                    "message": f"Invalid relationship column(s) based on file type '{file_type}' in the data model definition: {', '.join(f"'{col}'" for col in invalid_rel_cols)}. Either the node type is not found as a parent node for {file_type} or the key property for linking the parent node is not correct.",
                                }
                            )
                        else:
                            pass
            else:
                # there type column either not found, missing value found in type column, or mnultiple values found in type column, unable to decide the file type
                # no further format validation
                pass
        except Exception as e:
            print(f"Error processing {str(file_path)}: {e}")
            validation_errors.append(
                {
                    "level": "error",
                    "type": "file_read_error",
                    "message": f"Error reading the TSV file: {e}",
                }
            )
            # not to raise error which stops format validation for the rest of the files
        return validation_errors  # validation_errors can be an empty list if not violation is found

    def validate_tsv_files_format(
        self, file_path_list: list[str | Path]
    ) -> dict[str, list[dict[str, Any]]]:
        """
        ########################
        # FOR LOCAL VALIDATION #
        ########################
        Validates the format of TSV files in a specified list of file paths.

        Args:
            file_path_list: A list of paths to TSV files to be validated for format issues.
        Returns:
            dict[str, list[dict[str, Any]]]: A dictionary where the keys are file paths and the values are lists of dictionaries containing validation results for TSV format issues, including validity status and any warning/error messages.
        """
        validation_errors = {}
        for file in file_path_list:
            file_format_validation_errors = self.validate_tsv_format(file_path=file)
            if len(file_format_validation_errors) > 0:
                validation_errors[str(file)] = file_format_validation_errors
            else:
                pass
        return validation_errors

    @classmethod
    def if_record_exist_in_db(
        cls,
        driver: "GraphDatabase.driver",
        id_prop_value: str,
        id_prop_name: str = "guid",
        node_label: str | None = None,
    ) -> bool:
        """
        ########################
        # FOR VALIDATION IN DB #
        ########################
        A helper function to check if a node with specific id property value already exist in the database. This is used for validating relationship column value in the tsv file. If the parent node key prop value specified in the relationship column doesn't exist in the parent node file, we want to further check if this value exist in the database, which indicates this value can still be valid as long as it exist in the database.
        Raise ValueError: If more than one node with the specified id property value is found in the database. This indicates DB problem of having duplicate nodes with the same uuid/guid.

        Args:
            driver: GraphDatabase driver instance with proper connection to a graph database
            id_prop_name: The name of the id property, default is "guid"
            id_prop_value: The value of the id property to be checked
        Returns:
            bool: True if a node with the specified id property value exists in the database, False otherwise.
        """
        with driver.session() as session:
            match_clause = (
                f"MATCH (n:{node_label})" if node_label is not None else "MATCH (n)"
            )
            test_query = f"{match_clause} WHERE n.{id_prop_name} = $id_prop_value RETURN count(n) AS node_count"
            result = session.run(test_query, id_prop_value=id_prop_value)
            record = result.single()
            node_count = 0 if record is None else record["node_count"]
            if node_count > 1:
                raise ValueError(
                    f"Found {node_count} nodes in database with {id_prop_name}='{id_prop_value}'. Expected at most 1 unique node."
                )
            if node_count == 0:
                return False
            return True

    @classmethod
    def if_file_records_exist_in_db(
        cls,
        driver: "GraphDatabase.driver",
        file_path: str,
        id_prop_name: str = "guid",
        node_label: str | None = None,
        batch_size: int = 10000,
    ) -> dict[str, bool]:
        """
        ########################
        # FOR VALIDATION IN DB #
        ########################
        A helper function to check if multiple nodes with specific id property values exist in the database. The id property values are read from a TSV file.

        Args:
            driver: GraphDatabase driver instance with proper connection to a graph database
            file_path: Path to the TSV file containing id property values
            id_prop_name: The name of the id property, default is "guid"
            node_label: The label of the node in the database, default is None
            batch_size: Number of id property values to check in each batch query, default is 10000
        Returns:
            dict[str, bool]: A dictionary where keys are id property values and values are booleans indicating existence in the database.
        """
        # row number -> id value (row 2 is the first data row, since row 1 is the header)
        row_to_id: dict[int, str] = {}
        unique_ids: set[str] = set()
        # create this {rownum: id_value} dict to help identify the row and id value
        with open(file_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter="\t")
            if id_prop_name not in (reader.fieldnames or []):
                raise ValueError(
                    f"Column '{id_prop_name}' not found in file. "
                    f"Available columns: {reader.fieldnames}"
                )
            row_num = 1  # header is line 1
            for row in reader:
                row_num += 1
                val = row.get(id_prop_name)
                row_to_id[row_num] = val
                if val:
                    unique_ids.add(val)
        # Query the DB once per unique id, batched
        match_clause = (
            f"MATCH (n:{node_label})" if node_label is not None else "MATCH (n)"
        )
        query = f"""
            UNWIND $ids AS id_val
            OPTIONAL MATCH (n) WHERE n.{id_prop_name} = id_val
            WITH id_val, count(n) AS node_count
            RETURN id_val, node_count
        """.replace("MATCH (n)", match_clause)
        id_exists: dict[str, bool] = {}  # dict to store any node that exists in DB
        id_list = list(unique_ids)
        with driver.session() as session:
            for start in range(0, len(id_list), batch_size):
                batch = id_list[start : start + batch_size]
                for record in session.run(query, ids=batch):
                    id_val = record["id_val"]
                    node_count = record["node_count"]
                    if node_count > 1:
                        raise ValueError(
                            f"Found {node_count} nodes in database with "
                            f"{id_prop_name}='{id_val}'. Expected at most 1 unique node."
                        )
                    id_exists[id_val] = (
                        node_count == 1
                    )  # if node_count is 1, the value is True, the value if False if node_count == 0

        # Map every row back to its existence result
        return {
            row_num: (bool(val) and id_exists.get(val, False))
            for row_num, val in row_to_id.items()
        }

    @staticmethod
    def _extract_rows_with_existing_records(
        if_file_records_exist_return: dict[int, bool],
    ) -> list[int]:
        """
        A helper function to extract the row numbers from the dictionary returned by if_file_records_exist_in_db
        where the value is True, indicating that the record exists in the database.

        Args:
            if_file_records_exist_return (dict[int, bool]): A dictionary where keys are row numbers and values are booleans indicating existence in the database.

        Returns:
            list[int]: A list of row numbers where the corresponding record exists in the database.
        """
        return [
            row_num
            for row_num, exists in if_file_records_exist_return.items()
            if exists
        ]

    @classmethod
    def if_edge_exist_in_db(
        cls, driver: "GraphDatabase.driver", rel_dict_item: dict[str, Any]
    ) -> bool:
        """
        ########################
        # FOR VALIDATION IN DB #
        ########################
        A helper function to validate if a specifc edge exists in the database based on guids of src and dst node.
        An example of rel_dict_item would be:
        {
            "src_label": "sample",
            "src_id_prop": "guid",
            "src_id_value": "891e7e1a-9c25-4d25-a4b8-1517233f3f8f",
            "dst_label": "participant",
            "dst_id_prop": "guid",
            "dst_id_value": "395aa6ed-b912-483f-bf20-16e0313b21ae",
        }

        Args:
            driver (GraphDatabase.driver): GraphDatabase driver instance with proper connection to a graph database
            rel_dict_item (dict[str, Any]): A dictionary containing the details of the edge to be checked, including source and destination node labels, id properties, and id values.

        Returns:
            bool: True if the edge exists, False otherwise
        """
        with driver.session() as session:
            test_query = f"""
            MATCH (src:{rel_dict_item['src_label']})-[r]->(dst:{rel_dict_item['dst_label']})
            WHERE src.{rel_dict_item['src_id_prop']} = $src_id_value AND dst.{rel_dict_item['dst_id_prop']} = $dst_id_value
            RETURN count(r) AS edge_count
            """
            result = session.run(
                test_query,
                src_id_value=rel_dict_item["src_id_value"],
                dst_id_value=rel_dict_item["dst_id_value"],
            )
            record = result.single()
            edge_count = 0 if record is None else record["edge_count"]
            if edge_count == 0:
                return False
            elif edge_count == 1:
                return True
            else:
                raise ValueError(
                    f"Found {edge_count} edges in database between two data nodes. Expected 0 or 1 edge.\nSrc node {rel_dict_item['src_id_prop']}:{rel_dict_item['src_id_value']}\nDst node {rel_dict_item['dst_id_prop']}:{rel_dict_item['dst_id_value']}"
                )

    @classmethod
    def get_node_record_in_db(
        cls,
        driver: "GraphDatabase.driver",
        id_prop_value: str,
        id_prop_name: str = "guid",
    ) -> dict[str, Any] | None:
        """
        ########################
        # FOR VALIDATION IN DB #
        ########################
        A helper function to find a node record with specific id property value in the database. The return will be used to compare the record found in the submission file.
        We only expect one record found in the database since this is a check based on the unique id property value. If more than one record is found, it indicates there is duplicated data issue in the database, which should be fixed before the validation of submission files.
        # This helper function also cleans up the records by removing timestamp properties (["created", "updated"])
        # The return will contain id_property property value, such as guid

        An example return can be:
        {
          "dbgap_accession": "phs001228",
          "guid": "ecb2440c-c193-5229-b155-b32ec330981c", <---- guid is in returned record
          "promotion_status": [
            "Promote"
          ],
          "study_acronym": "KF-ESGR",
          "study_description": "example study description",
          "study_id": "phs001228",
          "study_name": "Gabriella Miller Kids First (GMKF) Pediatric Research Program in Susceptibility to Ewing Sarcoma Based on Germline Risk and Familial History of Cancer",
          "study_phase": "Completed"
        }

        Args:
            driver: GraphDatabase driver instance with proper connection to a graph database
            id_prop_name: The name of the id property, default is "guid"
            id_prop_value: The value of the id property to be checked
        Returns:
            dict[str, Any] | None: A dictionary containing the node record if found, None otherwise.
        """
        props_to_remove = ["created", "updated"]
        with driver.session() as session:
            # This query returns count and the first item of the nodes that match to the MATCH statement
            test_query = f"MATCH (n) WHERE n.{id_prop_name} = $id_prop_value RETURN count(n) AS node_count, head(collect(n)) AS node"
            result = session.run(test_query, id_prop_value=id_prop_value)
            record = result.single()
            node_count = 0 if record is None else record["node_count"]
            if node_count > 1:
                raise ValueError(
                    f"Found {node_count} nodes in database with {id_prop_name}='{id_prop_value}'. Expected exactly 0 or 1 node."
                )
            # if record is found, return the node properties as a dictionary, otherwise return None
            properties_value = dict(record["node"]) if node_count == 1 else None
            if properties_value is not None:
                for prop_to_remove in props_to_remove:
                    properties_value.pop(prop_to_remove, None)
            return properties_value

    @classmethod
    def get_file_records_in_db(
        cls,
        driver: "GraphDatabase.driver",
        file_path: str,
        id_prop_name: str = "guid",
        node_label: str | None = None,
        batch_size: int = 10000,
    ) -> dict[int, dict[str, Any] | None]:
        """
        ########################
        # FOR VALIDATION IN DB #
        ########################
        Batched version of get_node_record_in_db. Reads a TSV file and, for each data row,
        fetches the matching node record from the database using UNWIND so a large file
        only needs a handful of queries. Timestamp properties (["created", "updated"]) are
        stripped from each record. The returned record keeps the id property (e.g. guid).

        NOTE: Every data row appears in the return. If a row's id has no matching node in the DB
        (or the id is blank), its value is None.

        Raise ValueError: If any id value maps to more than one node in the database
            (indicates duplicate data that should be fixed before validation).

        Args:
            driver: GraphDatabase driver instance with proper connection.
            file_path: Path to the TSV file. Must contain a column named `id_prop_name`.
            id_prop_name: The name of the id property / column, default "guid".
            node_label: Optional node label to restrict the match.
            batch_size: How many unique ids to fetch per query.

        Returns:
            dict[int, dict[str, Any] | None]: Maps row number (data rows start at 2, i.e.
                line 1 is the header) to the node record dict, or None if not found.
        """
        props_to_remove = ["created", "updated"]

        # row number -> id value (row 2 is the first data row, since row 1 is the header)
        row_to_id: dict[int, str] = {}
        unique_ids: set[str] = set()

        with open(file_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter="\t")
            if id_prop_name not in (reader.fieldnames or []):
                raise ValueError(
                    f"Column '{id_prop_name}' not found in file. "
                    f"Available columns: {reader.fieldnames}"
                )
            row_num = 1  # header is line 1
            for row in reader:
                row_num += 1
                val = row.get(id_prop_name)
                row_to_id[row_num] = val
                if val:
                    unique_ids.add(val)

        # For each unique id, fetch count (to detect duplicates) and the node itself.
        match_clause = (
            f"MATCH (n:{node_label})" if node_label is not None else "MATCH (n)"
        )
        query = f"""
            UNWIND $ids AS id_val
            OPTIONAL MATCH (n) WHERE n.{id_prop_name} = id_val
            WITH id_val, count(n) AS node_count, head(collect(n)) AS node
            RETURN id_val, node_count, node
        """.replace("MATCH (n)", match_clause)

        id_to_record: dict[str, dict[str, Any] | None] = {}
        id_list = list(unique_ids)
        with driver.session() as session:
            for start in range(0, len(id_list), batch_size):
                batch = id_list[start : start + batch_size]
                for record in session.run(query, ids=batch):
                    id_val = record["id_val"]
                    node_count = record["node_count"]
                    if node_count > 1:
                        raise ValueError(
                            f"Found {node_count} nodes in database with "
                            f"{id_prop_name}='{id_val}'. Expected exactly 0 or 1 node."
                        )
                    if node_count == 1:
                        props = dict(record["node"])
                        for prop_to_remove in props_to_remove:
                            props.pop(prop_to_remove, None)
                        id_to_record[id_val] = props
                    else:
                        id_to_record[id_val] = None

        # Map every row back to its record (None for blank ids or ids not in the DB)
        return {
            row_num: (id_to_record.get(val) if val else None)
            for row_num, val in row_to_id.items()
        }

    @classmethod
    def get_record_outgoing_edges_in_db(
        cls,
        driver: "GraphDatabase.driver",
        id_prop_value: str,
        id_prop_name: str = "guid",
        node_label: str | None = None,
    ) -> list[dict[str, Any]]:
        """
        ########################
        # FOR VALIDATION IN DB #
        ########################
        A helper function to find all outgoing edges of a node with specific id property value in the database. The return will be used to compare the edges found in the submission file.
        If NO match is found in the database for target node, or NO outgoing edges are found for the target node, the function will return an empty list, [].
        # Example of dictionary item in the return list can be:
        [{
            "src_label": "sample",
            "src_id_prop": "guid",
            "src_id_value": "891e7e1a-9c25-4d25-a4b8-1517233f3f8f",
            "dst_label": "participant",
            "dst_id_prop": "guid",
            "dst_id_value": "395aa6ed-b912-483f-bf20-16e0313b21ae",
        }]

        Args:
            driver: GraphDatabase driver instance with proper connection to a graph database
            id_prop_name: The name of the id property, default is "guid"
            id_prop_value: The value of the id property to be checked
        Returns:
            list[dict[str, Any]]: A list of dictionaries, each containing edge information.
        """
        with driver.session() as session:
            match_clause = (
                f"MATCH (t:{node_label})-[]->(n)" if node_label else "MATCH (t)-[]->(n)"
            )
            test_query = f"""
            {match_clause}
            WHERE t.{id_prop_name} = $id_prop_value
            RETURN labels(t)[0] AS src_label, labels(n)[0] AS dst_label, collect(n.{id_prop_name}) AS values
            """
            result = session.run(test_query, id_prop_value=id_prop_value)
            outgoing_rels = []
            for record in result:
                src_label = record["src_label"]
                dst_label = record["dst_label"]
                values = record["values"]
                if (
                    dst_label is None
                ):  # if dst_label is None,it indicates there is no outgoing edge found for the target node
                    continue
                for dst_id_value in values:
                    outgoing_rels.append(
                        {
                            "src_label": src_label,
                            "src_id_prop": id_prop_name,
                            "src_id_value": id_prop_value,
                            "dst_label": dst_label,
                            "dst_id_prop": id_prop_name,
                            "dst_id_value": dst_id_value,
                        }
                    )
        return outgoing_rels

    @classmethod
    def get_file_records_outgoing_edges_in_db(
        cls,
        driver: "GraphDatabase.driver",
        file_path: str,
        id_prop_name: str = "guid",
        node_label: str | None = None,
        batch_size: int = 10000,
    ) -> dict[int, list[dict[str, Any]] | None]:
        """
        ########################
        # FOR VALIDATION IN DB #
        ########################
        Batched version of get_record_outgoing_edges_in_db. Reads a TSV file and, for each
        data row, finds all outgoing edges of the node with that row's id property value,
        using UNWIND so a large file only needs a handful of queries.

        Every data row appears in the return:
          - If the node does NOT exist in the DB (or the row's id is blank), value is None.
          - If the node exists but has no outgoing edges, value is an empty list [].
          - Otherwise value is a list of edge dicts (same shape as the original function).

        Example edge dict:
          {
            "src_label": "sample",
            "src_id_prop": "guid",
            "src_id_value": "891e7e1a-...",
            "dst_label": "participant",
            "dst_id_prop": "guid",
            "dst_id_value": "395aa6ed-...",
          }

        Raise ValueError: If any id value maps to more than one source node in the DB
            (indicates duplicate data that should be fixed before validation).

        Args:
            driver: GraphDatabase driver instance with proper connection.
            file_path: Path to the TSV file. Must contain a column named `id_prop_name`.
            id_prop_name: The name of the id property / column, default "guid".
            node_label: Optional label to restrict the source node match.
            batch_size: How many unique ids to process per query.

        Returns:
            dict[int, list[dict[str, Any]] | None]: Maps row number (data rows start at 2,
                i.e. line 1 is the header) to the list of outgoing edges, or None if the
                node was not found in the DB.
        """
        import csv

        # row number -> id value (row 2 is the first data row, since row 1 is the header)
        row_to_id: dict[int, str] = {}
        unique_ids: set[str] = set()

        with open(file_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter="\t")
            if id_prop_name not in (reader.fieldnames or []):
                raise ValueError(
                    f"Column '{id_prop_name}' not found in file. "
                    f"Available columns: {reader.fieldnames}"
                )
            row_num = 1  # header is line 1
            for row in reader:
                row_num += 1
                val = row.get(id_prop_name)
                row_to_id[row_num] = val
                if val:
                    unique_ids.add(val)

        # For each id: confirm the source node exists (node_exists), count matches to detect
        # duplicates (src_count), and collect outgoing edges grouped by destination label.
        # OPTIONAL MATCH on the outgoing pattern lets a node with no edges still show up.
        src_match = f"(t:{node_label})" if node_label else "(t)"
        query = f"""
            UNWIND $ids AS id_val
            OPTIONAL MATCH (t) WHERE t.{id_prop_name} = id_val
            WITH id_val, collect(t) AS srcs
            WITH id_val, size(srcs) AS src_count, head(srcs) AS t
            OPTIONAL MATCH (t)-[]->(n)
            WITH id_val, src_count, t,
                 labels(t)[0] AS src_label,
                 labels(n)[0] AS dst_label,
                 collect(n.{id_prop_name}) AS values
            RETURN id_val, src_count,
                   (t IS NOT NULL) AS node_exists,
                   head(collect(src_label)) AS src_label,
                   collect({{dst_label: dst_label, values: values}}) AS edge_groups
        """.replace("(t)", src_match, 1)

        id_to_edges: dict[str, list[dict[str, Any]] | None] = {}
        id_list = list(unique_ids)
        with driver.session() as session:
            for start in range(0, len(id_list), batch_size):
                batch = id_list[start : start + batch_size]
                for record in session.run(query, ids=batch):
                    id_val = record["id_val"]
                    src_count = record["src_count"]
                    node_exists = record["node_exists"]
                    src_label = record["src_label"]

                    if src_count > 1:
                        raise ValueError(
                            f"Found {src_count} nodes in database with "
                            f"{id_prop_name}='{id_val}'. Expected exactly 0 or 1 node."
                        )

                    if not node_exists:
                        id_to_edges[id_val] = None
                        continue

                    edges: list[dict[str, Any]] = []
                    for group in record["edge_groups"]:
                        dst_label = group["dst_label"]
                        if dst_label is None:  # node exists but has no outgoing edge
                            continue
                        for dst_id_value in group["values"]:
                            edges.append(
                                {
                                    "src_label": src_label,
                                    "src_id_prop": id_prop_name,
                                    "src_id_value": id_val,
                                    "dst_label": dst_label,
                                    "dst_id_prop": id_prop_name,
                                    "dst_id_value": dst_id_value,
                                }
                            )
                    id_to_edges[id_val] = edges

        # Map every row back to its edges (None for blank ids or ids not in the DB)
        return {
            row_num: (id_to_edges.get(val) if val else None)
            for row_num, val in row_to_id.items()
        }

    @classmethod
    def _read_file_parent_nodes_id(cls, file_path: str, id_prop_name: str = "guid", delimiter: str = ";") -> set[Tuple[str, str, str]]:
        """
        ########################
        # FOR VALIDATION IN DB #
        ########################
        A helper function to read the parent node id property values from a TSV file. This is used for validating relationship column value in the tsv file. If the parent node key prop value specified in the relationship column doesn't exist in the parent node file, we want to further check if this value exist in the database, which indicates this value can still be valid as long as it exist in the database.

        Args:
            file_path: Path to the TSV file containing id property values
            id_prop_name: The name of the id property, default is "guid"
            delimiter: The delimiter used in the TSV file, default is ";"
        Returns:
            list[tuple[str, str, str]]: A list of tuple found in the TSV file. Each tuple contains (parent_type, id_prop_name, id_prop_value).
        """
        # if the file type is a root node, the parent_node_ids can be empty
        seen = set()
        with open(file_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter="\t")
            parent_cols = [id_col for id_col in reader.fieldnames if id_col.endswith(f".{id_prop_name}")]
            if not parent_cols:
                return seen
            for row in reader:
                # we had make sure that the parent_col exists
                for parent_col in parent_cols:
                    parent_type = parent_col.split(".")[0].strip()
                    val = (row.get(parent_col) or "").strip()
                    val_list = [v.strip() for v in val.split(delimiter)]
                    for v in val_list:
                        if v != "":
                            seen.add((parent_type, id_prop_name, v))
        return seen

    @classmethod
    def if_parent_nodes_exist_in_db(
        cls,
        driver: "GraphDatabase.driver",
        file_path: str,
        id_prop_name: str = "guid",
        delimiter: str = ";",
        batch_size: int = 10000,
    ) -> dict[tuple[str, str, str], bool]:
        """
        ########################
        # FOR VALIDATION IN DB #
        ########################
        A helper function to check if parent node id property values exist in the database. The id property values are read from a TSV file.

        Raise ValueError: If any (parent_type, id_prop_name, id_prop_value) maps to more
        than one node in the database (indicates duplicate data to fix before
        validation).
        
        Args:
            driver: GraphDatabase driver instance with proper connection to a graph database
            file_path: Path to the TSV file containing id property values
            id_prop_name: The name of the id property, default is "guid"
            delimiter: The delimiter used in the TSV file, default is ";"
        Returns:
            dict[tuple[str, str, str], bool]: A dictionary where keys are tuples of (parent node type, id_prop_name, parent node id property value) and values are booleans indicating existence in the database.
        """
        # Step 1: read the file into unique (parent_type, id_prop_name, id_prop_value) tuples
        parent_node_ids = cls._read_file_parent_nodes_id(file_path=file_path, id_prop_name=id_prop_name, delimiter=delimiter)
        unique_triples = list(parent_node_ids)  # accepts a set or a list
        if not unique_triples:
            return {}

        # Step 2: group ids by (parent_type, id_prop_name) so each group shares one label
        # and one property name that we can bake into the query text.
        groups: dict[tuple[str, str], list[str]] = defaultdict(list)
        for parent_type, prop_name, prop_value in parent_node_ids:
            groups[(parent_type, prop_name)].append(prop_value)

        # Step 3: batch-check existence in the DB.
        _SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
        results: dict[tuple[str, str, str], bool] = {}
        with driver.session() as session:
            for (parent_type, prop_name), id_values in groups.items():
                # Validate before interpolating: labels/prop names can't be parameters, so
                # they enter the query as literal text. Reject anything that isn't a plain
                # identifier to prevent Cypher injection.
                if not _SAFE_IDENTIFIER.match(parent_type):
                    raise ValueError(f"Unsafe parent type for query: {parent_type!r}")
                if not _SAFE_IDENTIFIER.match(prop_name):
                    raise ValueError(f"Unsafe id property name for query: {prop_name!r}")

                # Label and property name are literal text; only the id values are parameters.
                query = f"""
                    UNWIND $ids AS id_val
                    OPTIONAL MATCH (n:{parent_type}) WHERE n.{prop_name} = id_val
                    WITH id_val, count(n) AS node_count
                    RETURN id_val, node_count
                """

                for start in range(0, len(id_values), batch_size):
                    batch = id_values[start : start + batch_size]
                    for record in session.run(query, ids=batch):
                        id_val = record["id_val"]
                        node_count = record["node_count"]
                        if node_count > 1:
                            raise ValueError(
                                f"Found {node_count} nodes in database with "
                                f"label='{parent_type}' and {prop_name}='{id_val}'. "
                                f"Expected at most 1 unique node."
                            )
                        results[(parent_type, prop_name, id_val)] = node_count == 1

        return results

    @classmethod
    def build_tsv_id_set(
        cls, tsv_file_list: list[str | Path], id_field: str = "guid"
    ) -> set[str]:
        """
        ########################
        # FOR VALIDATION IN DB #
        ########################
        Reads a list of TSV files once and collects all values of `id_field` into a set.
        Build this set a single time, then test membership with `id_value in the_set`
        (an O(1) lookup) instead of re-scanning the files for every id you want to check.

        Intended for the "check if dst node can be found in the tsv list" step of DB
        validation, when many ids must be checked against the same, possibly very large,
        set of files.

        Args:
            tsv_file_list: Paths to TSV files to read.
            id_field: The name of the id property/column, default "guid".

        Returns:
            set[str]: All non-empty id_field values found across the files.
        """
        id_set: set[str] = set()
        for file_path in tsv_file_list:
            try:
                tsv_id_iter = cls.read_tsv_records_id(
                    tsv_file_path=file_path, id_field=id_field
                )
                for _, id_dict in tsv_id_iter:
                    val = id_dict.get(id_field)
                    if val:  # skip None and empty strings
                        id_set.add(val)
            except Exception as e:
                print(f"Error processing {str(file_path)}: {e}")
                raise
        return id_set

    @classmethod
    def if_node_id_in_tsv_list(
        cls,
        id_value: str,
        tsv_id_set: set[str],
    ) -> bool:
        """
        ########################
        # FOR VALIDATION IN DB #
        ########################
        Returns True if id_value is present in a prebuilt set of TSV ids (see
        build_tsv_id_set). This is an O(1) membership test.

        Args:
            id_value: The id value to look for.
            tsv_id_set: A set of ids previously built from the TSV files.

        Returns:
            bool: True if id_value is in the set, False otherwise.
        """
        return id_value in tsv_id_set

    @classmethod
    def validate_tsv_in_db(
        cls,
        driver: "GraphDatabase.driver",
        tsv_file_path: str | Path,
        tsv_id_set: set[str],
        mdf_instance: MDFReader,
        id_prop_name: str,
        delimiter: str = ";",
        validation_mode: TestModeList = "Upsert",
    ) -> list[dict[str, Any]]:
        """
        ########################
        # FOR VALIDATION IN DB #
        ########################
        A helper function to validate  in the submission file against the record found in the database. This is used for validating relationship column value in the tsv file. If the parent node key prop value specified in the relationship column doesn't exist in the parent node file, we want to further check if this value exist in the database, which indicates this value can still be valid as long as it exist in the database. The validation will be based on the validation_mode defined as below:

        Args:
            tsv_file_path: The path to the TSV file to be validated.
            tsv_id_set: A set of ids previously built from the TSV files.
            id_prop_name: The name of the ID property in the TSV file.
            delimiter: The delimiter used in the TSV file, default is ";"
            validation_mode: The mode of validation to be performed, default is "Upsert"
        Returns:
            list[dict[str, Any]]: A list of dictionaries containing validation results for each row in the TSV file, including validity status and any warning/error messages.
        """
        validation_results = []
        passed_row_list = []
        processed_rows = 0
        progress_interval = 10000
        combined_record_reading = zip(
            cls.read_tsv_records_id(tsv_file_path=tsv_file_path, id_field=id_prop_name),
            cls.read_tsv_rels_id(
                tsv_file_path=tsv_file_path, id_field=id_prop_name, delimiter=delimiter
            ),
            cls.read_full_records_in_tsv(
                tsv_file_path=tsv_file_path, mdf_instance=mdf_instance, id_field=id_prop_name, delimiter=delimiter, keep_id_field=True
            )
        )
        # get file type from test file
        with open(tsv_file_path, mode="r", encoding="utf-8", newline="") as tsv_file:
            reader = csv.DictReader(tsv_file, delimiter="\t")
            # DictReader already consumes the header, so its first row is row 2.
            # index into the data rows: row_number 2 -> data index 0
            # if the row_number is less than 2 or past the end of the file, it will return None
            row = next(islice(reader, 0, 1), None)
            file_type = row.get("type") if row else None
        if file_type is None:
            raise ValueError(
                f"File {tsv_file_path} does not contain a 'type' column or the first data row is missing."
            )
        else:
            pass

        # batch checking if records in the tsv file already exist in the database
        # if the record doesn't exist, the returned dict will have None for the row number key
        # such as {5: None}
        file_records_if_exist_in_db = cls.if_file_records_exist_in_db(
            driver=driver,
            file_path=tsv_file_path,
            id_prop_name=id_prop_name,
            node_label=file_type,
        )
        print("Finished checking all records in the tsv file if they exist in the database.")
        # batch fetching file records in the tsv if the reocrd(via id_prop_name value) already exist in the db
        # if the record doesn't exist, the returned dict will have None for the row number key
        file_records_in_db = cls.get_file_records_in_db(
            driver=driver,
            file_path=tsv_file_path,
            id_prop_name=id_prop_name,
            node_label=file_type,
        )
        print("Finished fetching all database records in the tsv file that already exist in the database")
        # batch fetching outgoing edges of the record in the tsv if the record already exist in the db
        # if the record doesn't exist, the returned dict will have None for the row number key
        file_records_outgoing_edges_in_db = cls.get_file_records_outgoing_edges_in_db(
            driver=driver,
            file_path=tsv_file_path,
            id_prop_name=id_prop_name,
            node_label=file_type,
        )
        print("Finished fetching all outgoing edges of all record in file if they exist in the database")
        # fetch if parent nodes exist in the db for all parent nodes in a file
        parent_nodes_if_exist_in_db = cls.if_parent_nodes_exist_in_db(
            driver=driver,
            file_path=tsv_file_path,
            id_prop_name=id_prop_name,
            delimiter=delimiter)
        print("Finished checking all parent nodes found in the tsv file if they exist in the database")

        # iterate through each row in the tsv file and validate against db according to the validation mode
        print("Starting to validate each record")
        for row_num, (record_id, rels_in_record, row_record_in_file) in enumerate(
            combined_record_reading, start=2
        ):  # row_num starts from 2 because the first row is header and the second row is the first data row
            processed_rows += 1
            if progress_interval > 0 and processed_rows % progress_interval == 0:
                print(
                    f"Processed {processed_rows} rows from {tsv_file_path}...",
                    flush=True,
                )

            record_type, record_id_dict = record_id

            # row_pass is set to True at the begining of each row validation
            row_pass = True
            if_record_exist_in_db = file_records_if_exist_in_db.get(row_num, False)
            if validation_mode == "New":  # testing mode New
                if (
                    if_record_exist_in_db
                ):  # record already exists in db, this is not New data node
                    validation_results.append(
                        {
                            "row": row_num,
                            "record_type": record_type,
                            "validation_mode": validation_mode,
                            "level": "error",
                            "type": "record_already_exist_in_db",
                            "message": f"Record {record_type} with {id_prop_name}='{record_id_dict[id_prop_name]}' already exists in the database, but the test mode is 'New'.",
                            "hint": None,
                        }
                    )
                    row_pass = False
                    continue  # no need to further check for edges,
                else:  # record doesn't exist in db, this is correct as a new node
                    # because src node doesn't exist in db, all edges in file must be new to db
                    # we can check if the dst node exist in db or in the file, if dst node can't be found in either place, it will be an error because the edge can't be created
                    invalid_edge_hint = []
                    for rel in rels_in_record:
                        # test if dst node exist in db
                        if not parent_nodes_if_exist_in_db.get(
                            (rel["dst_label"], rel["dst_id_prop"], rel["dst_id_value"])
                        ):
                            # test if dst node exist in the tsv file. The edge is still valid if dst node can be created as new
                            if not cls.if_node_id_in_tsv_list(
                                tsv_id_set=tsv_id_set,
                                id_value=rel["dst_id_value"],
                            ):
                                # dst not exist in db or tsv (which means dst won't be created while loading
                                invalid_edge_hint.append(rel)
                            else:
                                pass
                        else:
                            pass
                    if len(invalid_edge_hint) > 0:
                        validation_results.append(
                            {
                                "row": row_num,
                                "record_type": record_type,
                                "validation_mode": validation_mode,
                                "level": "error",
                                "type": "invalid_edge_dst_node_not_found",
                                "message": f"Destination node(s) in edge(s) from record {record_type} with {id_prop_name}='{record_id_dict[id_prop_name]}' not found in the database or in the submission file: {len(invalid_edge_hint)}",
                                "hint": invalid_edge_hint,
                            }
                        )
                        row_pass = False
                    else:  # no invalid edge found. all dst node in edges can be found in either db or submission files
                        pass
            elif validation_mode == "Update":  # Update mode
                if (
                    not if_record_exist_in_db
                ):  # update mode only work with exisitng node
                    validation_results.append(
                        {
                            "row": row_num,
                            "record_type": record_type,
                            "validation_mode": validation_mode,
                            "level": "error",
                            "type": "record_not_found_in_db",
                            "message": f"Record {record_type} with {id_prop_name}='{record_id_dict[id_prop_name]}' not found in the database. This record is a new record to DB, but the test mode is 'Update'.",
                            "hint": None,
                        }
                    )
                    row_pass = False
                else:  # record already exist in DB
                    # check if any prop value in the record is different from the record in db
                    row_record_in_db = file_records_in_db[row_num]
                    # row_record_in_db can not be none becasue we already checked
                    if row_record_in_file != row_record_in_db:
                        record_diff = cls.record_comparison(
                            record_file=row_record_in_file,
                            record_db=row_record_in_db,
                            compare_mode="Update",
                        )
                        # we know that two records are different, so record_diff can not be empty
                        validation_results.append(
                            {
                                "row": row_num,
                                "record_type": record_type,
                                "validation_mode": validation_mode,
                                "level": "info",
                                "type": "record_prop_value_will_be_updated",
                                "message": f"Record {record_type} with {id_prop_name}='{record_id_dict[id_prop_name]}' will be updated in the database after update.",
                                "hint": record_diff,
                            }
                        )
                    else:
                        pass  # no prop value change, no need to give info message

                    # find all outgoing edges of the src node in db
                    outgoing_edges_in_db = file_records_outgoing_edges_in_db[row_num]
                    # unique edge in file for this src node
                    uniq_edges_in_file = [
                        i for i in rels_in_record if i not in outgoing_edges_in_db
                    ]
                    # unique edge in db for this src node
                    uniq_edges_in_db = [
                        i for i in outgoing_edges_in_db if i not in rels_in_record
                    ]
                    # if uniq_edges_in_db is not empty, these edges will be deleted after update,
                    if len(uniq_edges_in_db) > 0:
                        # give warnings to the validation results
                        validation_results.append(
                            {
                                "row": row_num,
                                "record_type": record_type,
                                "validation_mode": validation_mode,
                                "level": "warning",
                                "type": "edge_will_be_deleted",
                                "message": f"Edge(s) in DB from record {record_type} with {id_prop_name}='{record_id_dict[id_prop_name]}' will be deleted after update: {len(uniq_edges_in_db)}",
                                "hint": uniq_edges_in_db,
                            }
                        )
                    else:
                        pass  # no existing edge to be deleted

                    # among unique edges in file, check if it is new edge to be created
                    if (
                        len(uniq_edges_in_file) > 0
                    ):  # we need to check is dst node in edges exist in db
                        # to find if all edges in uniq_edges_in_file can be created in db
                        invalid_edges_hint = []
                        for edge in uniq_edges_in_file:
                            if not parent_nodes_if_exist_in_db.get(
                                (edge["dst_label"], edge["dst_id_prop"], edge["dst_id_value"])
                            ):
                                # dst not found in db, give error to the validation_results
                                # if dst does not exist, submitter should submit dst node to db through upsert or new mode first
                                # we DON'T test if dst exist in the submission files, because even if they can be found, the dst needs to be created as a New node in DB
                                invalid_edges_hint.append(edge)
                            else:
                                pass
                        # get the VALID uniq edges in file
                        valid_edges_in_file = [
                            edge
                            for edge in uniq_edges_in_file
                            if edge not in invalid_edges_hint
                        ]
                        if len(valid_edges_in_file) > 0:
                            validation_results.append(
                                {
                                    "row": row_num,
                                    "record_type": record_type,
                                    "validation_mode": validation_mode,
                                    "level": "info",
                                    "type": "new_edge_will_be_created",
                                    "message": f"Valid edge(s) in file from record {record_type} with {id_prop_name}='{record_id_dict[id_prop_name]}' will be created in the database after update: {len(valid_edges_in_file)}",
                                    "hint": valid_edges_in_file,
                                }
                            )
                        # if any invalid edge is found, give error and turn row_pass to False
                        if len(invalid_edges_hint) > 0:
                            validation_results.append(
                                {
                                    "row": row_num,
                                    "record_type": record_type,
                                    "validation_mode": validation_mode,
                                    "level": "error",
                                    "type": "invalid_edge_dst_node_not_found",
                                    "message": f"Destination node(s) in edge(s) from record {record_type} with {id_prop_name}='{record_id_dict[id_prop_name]}' not found in the database: {len(invalid_edges_hint)}",
                                    "hint": invalid_edges_hint,
                                }
                            )
                            row_pass = False
                        else:
                            pass
            elif validation_mode == "Upsert":  # Upsert mode
                # Upsert mode might create ERROR if the dst node of an edge can't be found in db or files
                # no need to check if the src node exists in db or not
                # because if yes, node will be updated, if no, node will be created in db
                if if_record_exist_in_db:
                    # if record exist in db, check if the record in db is different from the file
                    # check if any prop value in the record is different from the record in db
                    row_record_in_db = file_records_in_db[row_num]
                    if row_record_in_file != row_record_in_db:
                        record_diff = cls.record_comparison(
                            record_file=row_record_in_file,
                            record_db=row_record_in_db,
                            compare_mode="Upsert",
                        )
                        # record_diff can be empty
                        # the record_diff can be empty if the record in file is missing some prop values compared to the record in DB.
                        if all(v == {} for v in record_diff.values()):
                            props_missed_in_file = {
                                k: row_record_in_db[k]
                                for k in row_record_in_db.keys()
                                - row_record_in_file.keys()
                            }
                            validation_results.append(
                                {
                                    "row": row_num,
                                    "record_type": record_type,
                                    "validation_mode": validation_mode,
                                    "level": "warning",
                                    "type": "record_prop_value_stay_the_same",
                                    "message": f"Record {record_type} with {id_prop_name}='{record_id_dict[id_prop_name]}' in file is different from db. But the prop values will stay the same due to Upsert mode.",
                                    "hint": {
                                        "props_missed_in_file": props_missed_in_file
                                    },
                                }
                            )
                        else:
                            # when record_diff is not empty
                            validation_results.append(
                                {
                                    "row": row_num,
                                    "record_type": record_type,
                                    "validation_mode": validation_mode,
                                    "level": "info",
                                    "type": "record_prop_value_will_be_updated",
                                    "message": f"Record {record_type} with {id_prop_name}='{record_id_dict[id_prop_name]}' will be updated in the database after upsert.",
                                    "hint": record_diff,
                                }
                            )
                    else:
                        pass  # no prop value change, no need to give info message

                    # if node already exist in db, get all outgoing edges of the src node in db
                    outgoing_edges_in_db = file_records_outgoing_edges_in_db[row_num]
                    # unique edge in file for this src node
                    uniq_edges_in_file = [
                        i for i in rels_in_record if i not in outgoing_edges_in_db
                    ]
                    # unique edge in db for this src node
                    uniq_edges_in_db = [
                        i for i in outgoing_edges_in_db if i not in rels_in_record
                    ]
                    if len(uniq_edges_in_db) > 0:
                        # give warnings to the validation results
                        validation_results.append(
                            {
                                "row": row_num,
                                "record_type": record_type,
                                "validation_mode": validation_mode,
                                "level": "warning",
                                "type": "existing_edges_in_db",
                                "message": f"Existing edge(s) in DB (not noted in file) from record {record_type} with {id_prop_name}='{record_id_dict[id_prop_name]}' will be kept in the database after upsert: {len(uniq_edges_in_db)}",
                                "hint": uniq_edges_in_db,
                            }
                        )
                    else:
                        pass
                    if len(uniq_edges_in_file) > 0:
                        invalid_edges_hint = []
                        # check uniq_edges_in_file if dst node exist in db, if not, check if dst exist in the submission files
                        # if dst can be found in db or in the files, the edge can be established
                        for edge in uniq_edges_in_file:
                            if not parent_nodes_if_exist_in_db.get(
                                (edge["dst_label"], edge["dst_id_prop"], edge["dst_id_value"])
                                ):
                                # dst not found in db, check if dst can be found in the submission files
                                if not cls.if_node_id_in_tsv_list(
                                    tsv_id_set=tsv_id_set,
                                    id_value=edge["dst_id_value"]
                                ):
                                    # dst not found in db or in the submission files, give error to the validation_results
                                    invalid_edges_hint.append(edge)
                                else:
                                    pass
                            else:
                                pass
                        if len(invalid_edges_hint) > 0:
                            validation_results.append(
                                {
                                    "row": row_num,
                                    "record_type": record_type,
                                    "validation_mode": validation_mode,
                                    "level": "error",
                                    "type": "invalid_edge_dst_node_not_found",
                                    "message": f"Destination node(s) in edge(s) from record {record_type} with {id_prop_name}='{record_id_dict[id_prop_name]}' not found in the database or in the submission file: {len(invalid_edges_hint)}",
                                    "hint": invalid_edges_hint,
                                }
                            )
                            row_pass = False
                        else:
                            pass
                        valid_uniq_edges_in_file = [
                            edge
                            for edge in uniq_edges_in_file
                            if edge not in invalid_edges_hint
                        ]
                        if len(valid_uniq_edges_in_file) > 0:
                            validation_results.append(
                                {
                                    "row": row_num,
                                    "record_type": record_type,
                                    "validation_mode": validation_mode,
                                    "level": "info",
                                    "type": "new_edge_will_be_created",
                                    "message": f"Valid edge(s) in file from record {record_type} with {id_prop_name}='{record_id_dict[id_prop_name]}' will be created in the database after upsert: {len(valid_uniq_edges_in_file)}",
                                    "hint": valid_uniq_edges_in_file,
                                }
                            )
                        else:
                            pass
                    else:
                        pass
                else:  # if the node doesn't exist in db, it will be created as new node
                    # we only need to make sure the edges are valid
                    invalid_edge_hint = []
                    for rel in rels_in_record:
                        # test if dst node exist in db
                        if not parent_nodes_if_exist_in_db.get(
                            (rel["dst_label"], rel["dst_id_prop"], rel["dst_id_value"])
                        ):
                            # test if dst node exist in the tsv file. The edge is still valid if dst node can be created as new
                            if not cls.if_node_id_in_tsv_list(
                                tsv_id_set=tsv_id_set,
                                id_value=rel["dst_id_value"]
                            ):
                                # dst not exist in db or tsv (which means dst won't be created while loading
                                invalid_edge_hint.append(rel)
                            else:
                                pass
                        else:
                            pass
                    if len(invalid_edge_hint) > 0:
                        validation_results.append(
                            {
                                "row": row_num,
                                "record_type": record_type,
                                "validation_mode": validation_mode,
                                "level": "error",
                                "type": "invalid_edge_dst_node_not_found",
                                "message": f"Destination node(s) in edge(s) from record {record_type} with {id_prop_name}='{record_id_dict[id_prop_name]}' not found in the database or in the submission file: {len(invalid_edge_hint)}",
                                "hint": invalid_edge_hint,
                            }
                        )
                        row_pass = False
                    else:  # no invalid edge found. all dst node in edges can be found in either db or submission files
                        pass
            else:
                raise ValueError(
                    f"Invalid validation_mode '{validation_mode}' provided. Expected one of: 'New', 'Update', 'Upsert'."
                )
            if row_pass:
                passed_row_list.append(row_num)
        print(
            f"Finished validating {processed_rows} rows from {tsv_file_path}.",
            flush=True,
        )
        return passed_row_list, validation_results
