from bento_mdf import MDFDataValidator, MDFReader, MDF
from typing import Any, List
from pathlib import Path, PosixPath
import csv
import os
import pandas as pd
from uuid import UUID, uuid5
import hashlib
from collections.abc import Iterator
from src.parser import ModelParser

class Validator:
    def __init__(self, mdf: MDFReader):
        self.mdf = mdf
        self.model = self.mdf.model
        self.record_validator = MDFDataValidator(mdf=self.mdf)

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
    def generate_uuid5(project_name: str, subgraph_value: str, record_type: str, record_key_value: str) -> str:
        """
        Generate a UUID5 based on the project namespace, record type, and record key value.

        Args:
            project_name (str): The name of the project.
            record_type (str): The type of the record. e.g. "participant", "diagnosis", etc.
            record_key_value (str): The key value of the record. e.g. the value of "participant_id" column for a participant record, or the value of "diagnosis_id" for a diagnosis record.

        Returns:
            str: The generated UUID5 as a string.
        """
        if pd.isna(record_key_value) or record_key_value.strip() == "":
            return ""
        else:
            project_namespace = Validator.get_project_namespace(project_name)
            str_input = f"{subgraph_value}::{record_type}::{record_key_value}"
            str_uuid = uuid5(project_namespace, str_input)
        return str(str_uuid)

    @staticmethod
    def add_uuid_to_tsv_file(file_path: str | PosixPath, project_name: str, mdf: MDF, output_file_path: str, uuid_column: str = "guid") -> None:
        """
        Add a "guid" column to the TSV file with generated UUID5 values based on the project namespace, record type, and record key value.
        The output TSV will have the UUID column, such as "guid", and all relationship columns will be converted to UUID based on the parent type.
        The original relationship columns and the "subgraph" column will be removed from the output TSV.

        Args:
            file_path (str): The path to the input TSV file.
            project_name (str): The name of the project.
            subgraph_value (str): The value to be added to the record dict for subgraph key, which is also used for uuid generation.
            id_field_mapping (dict[str, str]): A dictionary mapping record types to their corresponding key field names. e.g. {"participant": "participant_id", "diagnosis": "diagnosis_id"}
            output_file_path (str): The path to save the output TSV file with the added "guid" column.
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
        )
        file_type = file_df["type"].iloc[0] # we only expect one type of a file

        # test if the subgraph column has any NA or more than one unique value
        if "subgraph" not in file_df.columns:
            raise KeyError(f"No subgraph column found in {file_path_str}, unable to generate uuid without subgraph value for namespace, please add a subgraph column with value")
        elif file_df["subgraph"].isna().any():
            raise ValueError(f"subgraph column in {file_path_str} contains NA value, unable to generate uuid without subgraph value for namespace, please add any missing subgraph value")
        elif file_df["subgraph"].nunique() > 1:
            raise ValueError(
                f"subgraph column in {file_path_str} contains more than one unique value. Only one unique subgraph value is expeted per file. Subgraph values found: {",".join(file_df["subgraph"].unique().tolist())}"
            )
        else:
            pass

        # take the subgraph value from the first record
        file_subgraph_value = file_df["subgraph"].iloc[0]
        # check if subgraph value is empty string or stirng with whitespace only
        if pd.isna(file_subgraph_value) or file_subgraph_value.strip() == "":
            raise ValueError(f"No subgraph value found in {file_path_str} at first record, unable to generate uuid without subgraph value for namespace, please add a subgraph value")
        else:
            pass

        file_type_key_prop = mdf.model.nodes[file_type].get_key_prop().handle

        # first write uuid column
        file_df[uuid_column] = file_df.apply(lambda row: Validator.generate_uuid5(project_name=project_name, subgraph_value=file_subgraph_value, record_type=file_type, record_key_value=row[file_type_key_prop]), axis=1)

        # second write guid for all relationship columns
        rel_col = [col for col in file_df.columns if "." in col]
        for col in rel_col:
            parent_type = col.split(".")[0]
            new_rel_col = parent_type + "." + uuid_column
            file_df[new_rel_col] = file_df.apply(lambda row: Validator.generate_uuid5(project_name=project_name, subgraph_value=file_subgraph_value, record_type=parent_type, record_key_value=row[col]), axis=1)

        # remove original relationship columns and subgraph column
        cols_to_remove = rel_col + ["subgraph"] if "subgraph" in file_df.columns else rel_col
        file_df.drop(columns=cols_to_remove, inplace=True)
        # write to new file in the given output file path
        file_df.to_csv(output_file_path, sep="\t", index=False)
        return None

    @staticmethod
    def record_prep(record_dict: dict, mdf: MDF, subgraph_col: str = "subgraph",id_field: str = "guid", delimiter: str = ";") -> dict:
        """
        Prepares a record dictionary for validation by removing certain keys and transform str to list for list type properties if needed
        Keys that need to be removed: "type",  "guid" and linkage keys which contain "."
        The file does not need to have this column, but if it does, we want to remove it before validation since it's not part of the model definition.

        Args:
            record_dict (dict): The original record dictionary.
            mdf (MDF): The MDF object containing the model definition.
            subgraph_col (str): The name of the column that indicates subgraph information. default is "subgraph".
            id_field (str): The name of the id field, default is "guid"
            delimiter (str): The delimiter used for list properties in the record dictionary. default is ";"

        Returns:
            dict: The prepared record dictionary.
        """
        record_type = record_dict["type"]

        # remove keys that are irrelavant for validation, including type, guid/id, and linkage related keys which contain "."
        remove_key_list = [key for key in record_dict.keys() if '.' in key or key in ["type", id_field, subgraph_col]]
        for key in remove_key_list:
            record_dict.pop(key)

        # remove key if value is empty or made of only whitespace
        key_to_remove = []
        for key in record_dict.keys():
            if record_dict[key] is None: # this only happens when a short row is read
                key_to_remove.append(key)
            elif record_dict[key].strip() == "": # when there is only a placeholder or string made of whitespace
                key_to_remove.append(key)
            else:
                pass
        for key in key_to_remove:
            record_dict.pop(key)

        # convert str to list if property type is list
        # the record_dict should only contain value that is not empty string
        for key in record_dict.keys():
            key_prop = mdf.model.nodes[record_type].props[key]
            key_prop_type = key_prop.value_domain
            if key_prop_type == "list":
                key_value = record_dict[key]
                key_value_list = [item.strip() for item in key_value.split(delimiter)]
                record_dict[key] = key_value_list
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
    def create_subgraph_dict(file_folder_path: str | PosixPath) -> dict[str, list[str]]:
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
    def add_subgrapgh_value_to_tsv(file_path: str | PosixPath, subgraph_vlaue: str) -> str:
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
        file_name = os.path.basename(file_path)
        new_file_name = file_name.replace(".tsv", f"_with_subgraph.tsv")
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
            file_df["subgraph"] = subgraph_vlaue
            file_df.to_csv(new_file_name, sep="\t", index=False, na_rep="")
            return new_file_name
        except Exception as e:
            print(f"Error reading {str(file_path)}: {e}")
            raise e

    @staticmethod
    def file_type_read(file_path_list: list[str | PosixPath]) -> dict[str, list[str]]:
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
                    raise KeyError(f"No 'type' column found in {str(file_path)}, unable to determine file type for categorization")
                elif file_df["type"].nunique() > 1:
                    raise ValueError(f"Multiple types found in 'type' column of {str(file_path)}, unable to determine file type for categorization. Types found: {','.join(file_df['type'].unique().tolist())}")
                elif file_df["type"].isna().any():
                    raise ValueError(f"'type' column in {str(file_path)} contains NA value, unable to determine file type for categorization, please add any missing type value")
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

    @classmethod
    def read_tsv_records(cls, tsv_file_path: str, mdf: MDF, subgraph_col: str = "subgraph", id_field: str = "guid", delimiter: str = ";") -> Iterator[tuple[str, dict[str, str]]]:
        """
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
                row_dict = cls.record_prep(row_dict, mdf, subgraph_col=subgraph_col, id_field=id_field, delimiter=delimiter)
                yield row_type, row_dict

    def validate_record(
        self, node_name: str, list_of_records: dict | List[dict]
    ) -> tuple[bool, dict[str, Any]]:
        """
        Validates node level data entries, such as participant record, from a node in dict format using the MDFDataValidator.


        Returns:
            bool: True if the data is valid, False otherwise.
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
            warning_error_messages["errors"] = (
                self.record_validator._validation_errors
            )
        return is_valid, warning_error_messages

    def validate_tsv_records(self, file_path: str, subgraph_col: str = "subgraph", id_field: str = "guid", delimiter: str = ";") -> dict[str, Any]:
        """
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
        row_num = 2 # the record starts frm the second row in the file
        for node_name, record in self.read_tsv_records(file_path, self.mdf, subgraph_col=subgraph_col, id_field=id_field, delimiter=delimiter):
            if node_name =="" and record == {}: # this happens when there is an emoty line
                validation_results.append(
                    {
                        "row": row_num,
                        "is_valid": False,
                        "messages": {
                            "warnings": {},
                            "errors": {
                                "0": [
                                    {
                                        "level": "error",
                                        "type": "missing",
                                        "loc": None,
                                        "msg": "This line is empty",
                                        "input": None,
                                        "url": "https://docs.pydantic.dev/2.12/errors/validation_errors/#missing",
                                    }
                                ]
                            },
                        },
                    }
                )
            elif node_name == "" and record != {}: # this happens when the "type" column is empty but other columns have value
                validation_results.append(
                    {
                        "row": row_num,
                        "is_valid": False,
                        "messages": {
                            "warnings": {},
                            "errors": {
                                "0": [
                                    {
                                        "level": "error",
                                        "type": "missing",
                                        "loc": ["type"],
                                        "msg": "Missing data type information in 'type' column, unable to validate this record",
                                        "input": None,
                                        "url": "https://docs.pydantic.dev/2.12/errors/validation_errors/#missing",
                                    }
                                ]
                            },
                        },
                    }
                )
            else: # use the validator_record to validate
                is_valid, messages = self.validate_record(node_name, record)

                if not is_valid:
                    validation_results.append({
                        "row": row_num,
                        "is_valid": is_valid,
                        "messages": messages
                    })
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
    def if_rel_valid(child_type : str, mdf: MDF, rel_to_test: str) -> bool:
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
                test_parent_node_key_prop_mdf = mdf.model.nodes[test_parent_node].get_key_prop().handle
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
            raise KeyError(f"Error when getting edges with {node_type} as source: {e}") from e
        except Exception as e:
            raise e
        return rel_multi

    def validate_tsv_rels(self, file_path_list: list[str | PosixPath], rel_delimiter: str =";") -> dict[str, Any]:     
        """
        Validates relationship records from a set of TSV files in a specified folder and returns the validation results for crosslinks.
        This function assumes the provided file list only contains files from the SAME SUBGRAPH. For instance, submission file for phs002790 study.
        
        This function can ONLY be used before the uuid5 generaiton. The relationship column name shoud be <parent_node>.<parent_node_key_prop>. 
        For instance, participant.participant_id

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
                )
                file_type = file_df["type"].iloc[0]
                rel_cols = [col for col in file_df.columns if "." in col]
                for rel_col in rel_cols:
                    # check if the relationship is valid base on the model definition
                    if not Validator.if_rel_valid(file_type, mdf, rel_col):
                        raise ValueError(f"Invalid relationship column {rel_col} found in file {str(file)}. Either the parent node is not found in MDF or the parent node key prop isn't correct")
                    else:
                        pass

                    # if the entire column is empty, skip this column
                    if file_df[rel_col].isna().all():
                        continue
                    else:
                        # not all values in the relationship column are empty
                        pass
                    
                    rel_multi = Validator.get_rel_multiplicity(file_type, rel_col.split(".")[0], mdf)
                    rel_col_parent, rel_col_parent_key_prop = rel_col.split(".")
                    parent_files = type_file_dict.get(rel_col_parent)
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
                        )
                        parent_key_values += parent_file_df[rel_col_parent_key_prop].dropna().tolist()
                    # only keep unique values in the parent_key_values
                    parent_key_values = list(set(parent_key_values))
                        
                    rel_col_values = file_df[rel_col]
                    for i in rel_col_values.index:
                        # row number is i+1
                        if pd.isna(rel_col_values[i]) or rel_col_values[i].strip() == "":
                            continue
                        else:
                            # only parse rel_col_values[i] when the relationship multiplicity is many_to_many or one_to_many, otherwise treat the whole value as one value
                            if rel_multi in ["many_to_many", "one_to_many"]:
                                i_value_list = [item.strip() for item in rel_col_values[i].split(rel_delimiter)]
                                for item in i_value_list:
                                    if item not in parent_key_values:
                                        # in case that key wasn't added to the validation result dict, initialize it with empty list
                                        if str(file) not in validation_results:
                                            validation_results[str(file)] = []
                                        validation_results[str(file)].append(
                                            {
                                                "row": i + 1, # add 1 to get the actual row number in the file since the index starts from 0
                                                "edge_column": rel_col,
                                                "invalid_value": item,
                                                "edge_src": file_type,
                                                "edge_dst": rel_col_parent,
                                                "message": f"Failed to find '{item}' in '{rel_col_parent}' file at column '{rel_col_parent_key_prop}': {', '.join(parent_files)}"
                                            }
                                        )
                                    else:
                                        pass
                            else:
                                if rel_col_values[i].strip() not in parent_key_values:
                                    if str(file) not in validation_results:
                                        validation_results[str(file)] = []
                                    validation_results[str(file)].append(
                                        {
                                            "row": i + 1, # add 1 to get the actual row number in the file since the index starts from 0
                                            "edge_column": rel_col,
                                            "invalid_value": rel_col_values[i],
                                            "edge_src": file_type,
                                            "edge_dst": rel_col_parent,
                                            "message": f"Failed to find '{rel_col_values[i]}' in '{rel_col_parent}' file at column '{rel_col_parent_key_prop}': {', '.join(parent_files)}"
                                        }
                                    )
                                else:
                                    pass
            except Exception as e:
                print(f"Error processing {str(file)}: {e}")
                raise e
        return validation_results



