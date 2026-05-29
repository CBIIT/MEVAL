import sys
import unittest
from pathlib import Path
import json
import tempfile
from uuid import UUID, uuid5

from bento_mdf import MDFReader

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from meval.parser import ModelParser
from meval.validator import Validator


class TestValidator(unittest.TestCase):
    MODEL_URL = PROJECT_ROOT / "tests" / "test_files" / "ccdi-dcc-model-test.yml"
    PROPS_URL = PROJECT_ROOT / "tests" / "test_files" / "ccdi-dcc-model-props-test.yml"

    @classmethod
    def setUpClass(cls) -> None:
        try:
            cls.model_parser = ModelParser(
                model_file=cls.MODEL_URL,
                props_file=cls.PROPS_URL,
                handle="ccdi_dcc",
            )
            cls.mdf_reader = MDFReader(
                cls.model_parser.model_file,
                cls.model_parser.props_file,
                handle="ccdi_dcc",
            )
            cls.validator = Validator(cls.mdf_reader)
        except Exception as error:
            raise unittest.SkipTest(
                f"Could not initialize model from remote URLs: {error}"
            ) from error

    def test_validate_tsv_records_survival_file(self) -> None:
        tsv_path = PROJECT_ROOT / "tests" / "test_files" / "survival_test.tsv"

        results = self.validator.validate_tsv_records(str(tsv_path))

        self.assertIsInstance(results, list)
        self.assertGreater(len(results), 0)
        self.assertEqual(len(results), 8)  # we expect 6 invalid records in this test file

        # first row error
        first_issue = results[0]
        self.assertIn("row", first_issue)
        self.assertEqual(first_issue["row"], 3) # row number of first record with error
        self.assertIn("is_valid", first_issue)
        self.assertIn("messages", first_issue)
        self.assertFalse(first_issue["is_valid"])
        self.assertIn("errors", first_issue["messages"])

        # emtpy line check
        last_issue = results[-1]
        self.assertIn("This line is empty", last_issue["messages"]["errors"][0]["msg"])

        # enum check
        self.assertIn("Eye Pain", results[6]["messages"]["warnings"][0]["input"])

    def test_read_tsv_records_survival_file(self) -> None:
        tsv_path = PROJECT_ROOT / "tests" / "test_files" / "survival_test.tsv"

        records = list(self.validator.read_tsv_records(str(tsv_path), self.mdf_reader, id_field="guid"))

        self.assertIsInstance(records, list)
        self.assertEqual(len(records), 9)

        # first record is parsed and cleaned
        first_node_name, first_record = records[0]
        self.assertEqual(first_node_name, "survival")
        self.assertNotIn("type", first_record)
        self.assertNotIn("guid", first_record)
        self.assertNotIn("participant.guid", first_record)
        self.assertEqual(first_record["survival_id"], "survival_101")

        # list-type values are split and stripped
        second_node_name, second_record = records[1]
        self.assertEqual(second_node_name, "survival")
        self.assertEqual(
            second_record["adverse_event"],
            ["Acute Coronary Syndrome", "Alopecia"],
        )

        # empty trailing line is represented as an empty record
        last_node_name, last_record = records[-1]
        self.assertEqual(last_node_name, "")
        self.assertEqual(last_record, {})

    def test_get_project_namespace(self) -> None:
        project_name = "ccdi_dcc"

        namespace = Validator.get_project_namespace(project_name)

        self.assertIsInstance(namespace, UUID)
        self.assertEqual(str(namespace), "267de2c8-1884-2485-5561-06ad836cdc42")

    def test_generate_uuid5(self) -> None:
        project_name = "ccdi_dcc"
        subgraph_value = "phs001"
        record_type = "participant"
        record_key_value = "participant_1"

        generated = Validator.generate_uuid5(
            project_name=project_name,
            subgraph_value=subgraph_value,
            record_type=record_type,
            record_key_value=record_key_value,
        )

        expected = str(
            uuid5(
                Validator.get_project_namespace(project_name),
                f"{subgraph_value}::{record_type}::{record_key_value}",
            )
        )
        self.assertEqual(generated, expected)
        # if the record_key_value is empty, the guid should be empty  string
        self.assertEqual(
            Validator.generate_uuid5(
                project_name=project_name,
                subgraph_value=subgraph_value,
                record_type=record_type,
                record_key_value="",
            ),
            "",
        )

    def test_add_uuid_to_tsv_file(self) -> None:
        project_name = "ccdi_dcc"
        with tempfile.TemporaryDirectory() as tmp_dir:
            input_path = Path(tmp_dir) / "participant.tsv"
            output_path = Path(tmp_dir) / "participant_with_uuid.tsv"

            input_path.write_text(
                "type\tparticipant_id\tsex\tsubgraph\n"
                "participant\tP001\tMale\tphs001\n"
                "participant\tP002\tFemale\tphs001\n",
                encoding="utf-8",
            )

            Validator.add_uuid_to_tsv_file(
                file_path=input_path,
                project_name=project_name,
                mdf=self.mdf_reader,
                output_file_path=str(output_path),
                uuid_column="guid",
            )

            self.assertTrue(output_path.exists())
            output_lines = output_path.read_text(encoding="utf-8").strip().splitlines()
            header = output_lines[0].split("\t")
            self.assertIn("guid", header)
            self.assertNotIn("subgraph", header)

            idx_participant_id = header.index("participant_id")
            idx_guid = header.index("guid")

            first_row = output_lines[1].split("\t")
            second_row = output_lines[2].split("\t")

            expected_first_guid = Validator.generate_uuid5(
                project_name=project_name,
                subgraph_value="phs001",
                record_type="participant",
                record_key_value=first_row[idx_participant_id],
            )
            expected_second_guid = Validator.generate_uuid5(
                project_name=project_name,
                subgraph_value="phs001",
                record_type="participant",
                record_key_value=second_row[idx_participant_id],
            )

            self.assertEqual(first_row[idx_guid], expected_first_guid)
            self.assertEqual(second_row[idx_guid], expected_second_guid)

    def test_record_prep(self) -> None:
        # this is a quick test to make sure the record_prep function works as expected
        # we will test it with a simple record that has a missing required field and an empty line
        test_record = {
            "survival_id": "survival_001",
            "last_known_survival_status": "Dead",
            "age_at_last_known_survival_status": "1234",
            "adverse_event": "Back Pain ;  Blurred Vision", # heading whitespace
            "guid": "dsahuidhsuai",
            "type": "survival",
            "participant.guid": "rhuirhtutir"
        }

        prep_record = self.validator.record_prep(test_record, self.mdf_reader, id_field="guid", delimiter=";")

        self.assertNotIn("type", prep_record)
        self.assertNotIn("guid", prep_record)
        self.assertNotIn("participant.guid", prep_record)
        self.assertEqual(2, len(prep_record["adverse_event"]))
        self.assertEqual(prep_record["adverse_event"][1], "Blurred Vision")
        self.assertEqual(prep_record["adverse_event"][0], "Back Pain")

    def test_validate_record(self) -> None:
        # this is a quick test to make sure the validate_record function works as expected
        # we will test it with a simple record that has a missing required field and an empty line
        test_record = {
            "survival_id": "survival_001",
            "last_known_survival_status": "Dead",
            "age_at_last_known_survival_status": "1234",
            "adverse_event": ["Back Pain", "Wrong enum value"],
        }

        is_valid, messages = self.validator.validate_one_record("survival", test_record)

        self.assertFalse(is_valid)
        self.assertIn("errors", messages)
        self.assertEqual(1, len(messages["warnings"]))
        self.assertEqual("enum", messages["warnings"][0]["type"])

    def test_validate_tsv_format_participant_fixtures(self) -> None:
        base_dir = PROJECT_ROOT / "tests" / "test_files" / "tsv_format_files"

        valid_file = base_dir / "participant_valid_format.tsv"
        missing_type_col_file = base_dir / "participant_missing_type_col.tsv"
        missing_required_col_file = base_dir / "participant_missing_required_col.tsv"
        invalid_rel_col_file = base_dir / "participant_invalid_rel_col.tsv"
        missing_rel_col_file = base_dir / "participant_missing_rel_col.tsv"

        # valid format file should return no errors
        valid_errors = self.validator.validate_tsv_format(valid_file)
        self.assertEqual(valid_errors, [])

        # missing type column stops further checks
        missing_type_errors = self.validator.validate_tsv_format(missing_type_col_file)
        self.assertEqual(len(missing_type_errors), 1)
        self.assertEqual(missing_type_errors[0]["type"], "missing_column")

        # missing required columns and an unsupported property column
        missing_required_errors = self.validator.validate_tsv_format(missing_required_col_file)
        missing_required_error_types = {item["type"] for item in missing_required_errors}
        self.assertIn("missing_required_column", missing_required_error_types)
        self.assertIn("invalid_property_column", missing_required_error_types)

        # wrong relationship column and an unsupported property column
        invalid_rel_errors = self.validator.validate_tsv_format(invalid_rel_col_file)
        invalid_rel_error_types = {item["type"] for item in invalid_rel_errors}
        self.assertIn("invalid_relationship_column", invalid_rel_error_types)
        self.assertIn("invalid_property_column", invalid_rel_error_types)

        # no relationship column for non-root type and an unsupported property column
        missing_rel_errors = self.validator.validate_tsv_format(missing_rel_col_file)
        missing_rel_error_types = {item["type"] for item in missing_rel_errors}
        self.assertIn("missing_relationship_column", missing_rel_error_types)
        self.assertIn("invalid_property_column", missing_rel_error_types)

    def test_validate_tsv_files_format_participant_fixtures(self) -> None:
        base_dir = PROJECT_ROOT / "tests" / "test_files" / "tsv_format_files"

        valid_file = base_dir / "participant_valid_format.tsv"
        missing_type_col_file = base_dir / "participant_missing_type_col.tsv"
        missing_required_col_file = base_dir / "participant_missing_required_col.tsv"
        invalid_rel_col_file = base_dir / "participant_invalid_rel_col.tsv"
        missing_rel_col_file = base_dir / "participant_missing_rel_col.tsv"

        file_list = [
            valid_file,
            missing_type_col_file,
            missing_required_col_file,
            invalid_rel_col_file,
            missing_rel_col_file,
        ]

        validation_result = self.validator.validate_tsv_files_format(file_list)

        self.assertIsInstance(validation_result, dict)
        self.assertNotIn(str(valid_file), validation_result)

        self.assertIn(str(missing_type_col_file), validation_result)
        self.assertEqual(validation_result[str(missing_type_col_file)][0]["type"], "missing_column")

        self.assertIn(str(missing_required_col_file), validation_result)
        self.assertIn(
            "missing_required_column",
            {item["type"] for item in validation_result[str(missing_required_col_file)]},
        )

        self.assertIn(str(invalid_rel_col_file), validation_result)
        self.assertIn(
            "invalid_relationship_column",
            {item["type"] for item in validation_result[str(invalid_rel_col_file)]},
        )

        self.assertIn(str(missing_rel_col_file), validation_result)
        self.assertIn(
            "missing_relationship_column",
            {item["type"] for item in validation_result[str(missing_rel_col_file)]},
        )

    def test_validate_tsv_uniq_entry_with_duplicates_across_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            file_a = Path(tmp_dir) / "participant_a.tsv"
            file_b = Path(tmp_dir) / "participant_b.tsv"

            file_a.write_text(
                "type\tparticipant_id\tsex\n"
                "participant\tP001\tMale\n"
                "participant\tP002\tFemale\n",
                encoding="utf-8",
            )
            file_b.write_text(
                "type\tparticipant_id\tsex\n"
                "participant\tP001\tMale\n"
                "participant\tP003\tFemale\n",
                encoding="utf-8",
            )

            duplicated = self.validator.validate_tsv_uniq_entry([str(file_a), str(file_b)])

            self.assertEqual(len(duplicated), 2)
            self.assertSetEqual(
                {item["file_path"] for item in duplicated},
                {str(file_a), str(file_b)},
            )
            self.assertTrue(all(item["type"] == "participant" for item in duplicated))
            self.assertTrue(all(item["key_prop"] == "participant_id" for item in duplicated))
            self.assertTrue(all(item["key_prop_value"] == "P001" for item in duplicated))
            self.assertEqual(sorted(item["row"] for item in duplicated), [2, 2])

    def test_validate_tsv_uniq_entry_without_duplicates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            file_a = Path(tmp_dir) / "participant_a.tsv"
            file_b = Path(tmp_dir) / "participant_b.tsv"

            file_a.write_text(
                "type\tparticipant_id\tsex\n"
                "participant\tP100\tMale\n"
                "participant\tP101\tFemale\n",
                encoding="utf-8",
            )
            file_b.write_text(
                "type\tparticipant_id\tsex\n"
                "participant\tP102\tMale\n"
                "participant\tP103\tFemale\n",
                encoding="utf-8",
            )

            duplicated = self.validator.validate_tsv_uniq_entry([str(file_a), str(file_b)])

            self.assertEqual(duplicated, [])

    def test_validate_tsv_rels_reports_rows_without_any_relationship_values(self) -> None:
        base_dir = PROJECT_ROOT / "tests" / "test_files" / "rel_test_files"

        consent_group_file = base_dir / "test_rel_consent_group.tsv"

        with tempfile.TemporaryDirectory() as tmp_dir:
            participant_file = Path(tmp_dir) / "test_rel_participant_with_empty_relationship_row.tsv"
            participant_file.write_text(
                "type\tconsent_group.consent_group_id\tparticipant_id\trace\tsex_at_birth\toccupation\tcrdc_id\n"
                "participant\tphs000123_GRU\tPT_VALID_001\tWhite\tFemale\t\t\n"
                "participant\t\tPT_INVALID_002\tAsian\tMale\t\t\n",
                encoding="utf-8",
            )

            rel_results = self.validator.validate_tsv_rels(
                [consent_group_file, participant_file],
                rel_delimiter=";",
            )

            self.assertIn(str(participant_file), rel_results)
            missing_link_errors = [
                item
                for item in rel_results[str(participant_file)]
                if "Missing relationship value" in item["message"]
            ]

            self.assertEqual(len(missing_link_errors), 1)
            self.assertEqual(missing_link_errors[0]["row"], 3)
            self.assertEqual(missing_link_errors[0]["edge_src"], "participant")
            self.assertEqual(missing_link_errors[0]["edge_column"], "N/A")
            self.assertEqual(missing_link_errors[0]["invalid_value"], "N/A")
if __name__ == "__main__":
    unittest.main()
