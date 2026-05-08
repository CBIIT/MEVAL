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
        file_path: str | PosixPath,
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
    def add_subgrapgh_value_to_tsv(
        file_path: str | PosixPath, subgraph_vlaue: str, output_file_path: str
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

    def validate_records(
        self, node_name: str, list_of_records: List[dict]
    ) -> tuple[bool, dict[str, Any]]:
        """
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
            ):  # this happens when there is an emoty line
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
        self, file_path_list: list[str | PosixPath], rel_delimiter: str = ";"
    ) -> dict[str, Any]:
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
                        )
                        parent_key_values += (
                            parent_file_df[rel_col_parent_key_prop].dropna().tolist()
                        )
                    # only keep unique values in the parent_key_values
                    parent_key_values = list(set(parent_key_values))

                    rel_col_values = file_df[rel_col]
                    for i in rel_col_values.index:
                        # row number is i+2 since the index starts from 0 and the record starts from the second row in the file
                        if (
                            pd.isna(rel_col_values[i])
                            or rel_col_values[i].strip() == ""
                        ):
                            continue
                        else:
                            # only parse rel_col_values[i] when the relationship multiplicity is many_to_many or one_to_many, otherwise treat the whole value as one value
                            if rel_multi in ["many_to_many", "one_to_many"]:
                                i_value_list = [
                                    item.strip()
                                    for item in rel_col_values[i].split(rel_delimiter)
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
                                if rel_col_values[i].strip() not in parent_key_values:
                                    if str(file) not in validation_results:
                                        validation_results[str(file)] = []
                                    validation_results[str(file)].append(
                                        {
                                            "row": i
                                            + 2,  # add 2 to get the actual row number in the file since the index starts from 0 and the record starts from the second row in the file
                                            "edge_column": rel_col,
                                            "invalid_value": rel_col_values[i],
                                            "edge_src": file_type,
                                            "edge_dst": rel_col_parent,
                                            "message": f"Failed to find '{rel_col_values[i]}' in '{rel_col_parent}' file at column '{rel_col_parent_key_prop}': {', '.join(parent_files)}",
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
        file_path: str | PosixPath, key_prop: str, chunk_size: int = 5000
    ) -> Iterator[str]:
        """Read the key prop values of a tsv file

        Args:
            file_path (str | PosixPath): file path or filepath object
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
        self, file_path_list: list[str | PosixPath]
    ) -> list[dict[str, Any]]:
        """
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

    def validate_tsv_format(self, file_path: str | PosixPath) -> list[dict[str, Any]]:
        """
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
        self, file_path_list: list[str | PosixPath]
    ) -> dict[str, list[dict[str, Any]]]:
        """
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
