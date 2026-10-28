import sys
import unittest
from pathlib import Path
import json
import tempfile
from types import SimpleNamespace
from uuid import UUID, uuid5
from unittest.mock import ANY, MagicMock, patch

from bento_mdf import MDFReader

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from meval.parser import ModelParser
from meval.validator import (
    DatabaseValidator,
    RemoteValidator,
    LocalValidator,
    ValidatorUtilities,
)

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
            cls.validator = LocalValidator(cls.mdf_reader)
        except Exception as error:
            raise unittest.SkipTest(
                f"Could not initialize model from remote URLs: {error}"
            ) from error

    @staticmethod
    def _build_driver_with_session(session: MagicMock) -> MagicMock:
        driver = MagicMock()
        session_cm = MagicMock()
        session_cm.__enter__.return_value = session
        session_cm.__exit__.return_value = None
        driver.session.return_value = session_cm
        return driver

    @staticmethod
    def _write_survival_tsv(file_path: Path, participant_guid_value: str = "participant-guid-001") -> None:
        file_path.write_text(
            "type\tguid\tsurvival_id\tlast_known_survival_status\tage_at_last_known_survival_status\tadverse_event\tparticipant.guid\n"
            f"survival\tsurvival-guid-001\tsurvival_001\tDead\t1234\tBack Pain ;  Blurred Vision\t{participant_guid_value}\n",
            encoding="utf-8",
        )

    def test_validate_one_record_allows_warning_only_values(self) -> None:
        warning_record = {
            "survival_id": "survival_001",
            "last_known_survival_status": "Dead",
            "age_at_last_known_survival_status": 1234,
            "adverse_event": ["Back Pain", "Eye Pain"],
            "guid": "survival-guid-001",
        }

        is_valid, messages = self.validator.validate_one_record("survival", warning_record)

        self.assertTrue(is_valid)
        self.assertIn("warnings", messages)
        self.assertIn("errors", messages)
        self.assertEqual(messages["errors"], [])
        self.assertTrue(any(msg.get("level") == "warning" for msg in messages["warnings"]))

    def test_validate_one_record_returns_error_when_record_is_invalid(self) -> None:
        invalid_record = {
            "survival_id": "survival_001",
            "last_known_survival_status": "Dead",
            "age_at_last_known_survival_status": "not-a-number",
            "adverse_event": ["Back Pain", "Blurred Vision"],
            "guid": "survival-guid-001"
        }

        is_valid, messages = self.validator.validate_one_record("survival", invalid_record)

        self.assertFalse(is_valid)
        self.assertTrue(messages["errors"])
        self.assertIn("errors", messages)

    def test_validate_records_allows_warning_only_lists(self) -> None:
        valid_record = {
            "survival_id": "survival_101",
            "last_known_survival_status": "Dead",
            "age_at_last_known_survival_status": 1234,
            "adverse_event": ["Back Pain", "Blurred Vision"],
            "guid": "survival-guid-101",
        }
        warning_record = {
            "survival_id": "survival_102",
            "last_known_survival_status": "Dead",
            "age_at_last_known_survival_status": 5678,
            "adverse_event": ["Back Pain", "Eye Pain"],
            "guid": "survival-guid-102",
        }

        is_valid, messages = self.validator.validate_records("survival", [valid_record, warning_record])

        self.assertTrue(is_valid)
        self.assertIn("warnings", messages)
        self.assertIn("errors", messages)
        self.assertTrue(messages["warnings"])

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
        self.assertTrue(first_issue["is_valid"]) # only warnings found in the first issue
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

        namespace = ValidatorUtilities.get_project_namespace(project_name)

        self.assertIsInstance(namespace, UUID)
        self.assertEqual(str(namespace), "e3ff524e-a9df-bd30-2ea8-55ce08b3b27a")

    def test_generate_uuid5(self) -> None:
        project_name = "ccdi_dcc"
        subgraph_value = "phs001"
        record_type = "participant"
        record_key_value = "participant_1"

        generated = ValidatorUtilities.generate_uuid5(
            project_name=project_name,
            subgraph_value=subgraph_value,
            record_type=record_type,
            record_key_value=record_key_value,
        )

        expected = str(
            uuid5(
                ValidatorUtilities.get_project_namespace(project_name),
                f"{subgraph_value}::{record_type}::{record_key_value}",
            )
        )
        self.assertEqual(generated, expected)
        # if the record_key_value is empty, the guid should be empty  string
        self.assertEqual(
            ValidatorUtilities.generate_uuid5(
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

            ValidatorUtilities.add_uuid_to_tsv_file(
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

            expected_first_guid = ValidatorUtilities.generate_uuid5(
                project_name=project_name,
                subgraph_value="phs001",
                record_type="participant",
                record_key_value=first_row[idx_participant_id],
            )
            expected_second_guid = ValidatorUtilities.generate_uuid5(
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

    def test_record_prep_removes_empty_values_and_converts_numbers(self) -> None:
        test_record = {
            "type": "survival",
            "guid": "survival-guid-001",
            "survival_id": "survival_001",
            "last_known_survival_status": "Dead",
            "age_at_last_known_survival_status": "1234",
            "adverse_event": "Back Pain ;  Blurred Vision",
            "participant.guid": "participant-guid-001",
            "notes": "   ",
            "subgraph": "phs001",
        }

        prepared = ValidatorUtilities.record_prep(
            test_record,
            mdf=self.mdf_reader,
            subgraph_col="subgraph",
            id_field="guid",
            delimiter=";",
        )

        self.assertEqual(prepared["age_at_last_known_survival_status"], 1234)
        self.assertEqual(prepared["adverse_event"], ["Back Pain", "Blurred Vision"])
        self.assertNotIn("guid", prepared)
        self.assertNotIn("participant.guid", prepared)
        self.assertNotIn("subgraph", prepared)
        self.assertNotIn("notes", prepared)

    def test_read_record_by_row_in_tsv_keeps_or_drops_id_field(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tsv_path = Path(tmp_dir) / "survival.tsv"
            self._write_survival_tsv(tsv_path)

            without_id = ValidatorUtilities.read_record_by_row_in_tsv(
                tsv_file_path=str(tsv_path),
                row_number=2,
                mdf_instance=self.mdf_reader,
                keep_id_field=False,
                id_field="guid",
            )
            with_id = ValidatorUtilities.read_record_by_row_in_tsv(
                tsv_file_path=str(tsv_path),
                row_number=2,
                mdf_instance=self.mdf_reader,
                keep_id_field=True,
                id_field="guid",
            )

            self.assertEqual(without_id["survival_id"], "survival_001")
            self.assertEqual(without_id["age_at_last_known_survival_status"], 1234)
            self.assertEqual(without_id["adverse_event"], ["Back Pain", "Blurred Vision"])
            self.assertNotIn("guid", without_id)

            self.assertEqual(with_id["guid"], "survival-guid-001")
            self.assertNotIn("participant.guid", with_id)

    def test_read_tsv_rels_id_splits_multiple_relationship_values(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tsv_path = Path(tmp_dir) / "sample.tsv"
            tsv_path.write_text(
                "type\tguid\tsample_id\tparticipant.guid\tstudy.guid\n"
                "sample\tsample-guid-001\tsample_001\tparticipant-guid-001 ; participant-guid-002\tstudy-guid-001\n",
                encoding="utf-8",
            )

            rel_rows = list(ValidatorUtilities.read_tsv_rels_id(str(tsv_path), id_field="guid", delimiter=";"))

            self.assertEqual(len(rel_rows), 1)
            self.assertEqual(len(rel_rows[0]), 3)
            self.assertEqual(rel_rows[0][0]["src_label"], "sample")
            self.assertEqual(rel_rows[0][0]["dst_label"], "participant")
            self.assertEqual(rel_rows[0][0]["dst_id_value"], "participant-guid-001")
            self.assertEqual(rel_rows[0][1]["dst_id_value"], "participant-guid-002")
            self.assertEqual(rel_rows[0][2]["dst_label"], "study")

    def test_if_record_exist_in_db_returns_boolean_and_raises_on_duplicates(self) -> None:
        session = MagicMock()
        result = MagicMock()
        session.run.return_value = result
        driver = self._build_driver_with_session(session)

        result.single.return_value = {"node_count": 0}
        self.assertFalse(
            RemoteValidator.if_record_exist_in_db(
                driver, "missing-guid", node_label="participant"
            )
        )

        result.single.return_value = {"node_count": 1}
        self.assertTrue(
            RemoteValidator.if_record_exist_in_db(
                driver, "present-guid", node_label="participant"
            )
        )

        result.single.return_value = {"node_count": 2}
        with self.assertRaises(ValueError):
            RemoteValidator.if_record_exist_in_db(
                driver, "duplicate-guid", node_label="participant"
            )

    def test_if_edge_exist_in_db_returns_boolean_and_raises_on_duplicates(self) -> None:
        session = MagicMock()
        result = MagicMock()
        session.run.return_value = result
        driver = self._build_driver_with_session(session)
        rel_dict = {
            "src_label": "sample",
            "src_id_prop": "guid",
            "src_id_value": "sample-guid-001",
            "dst_label": "participant",
            "dst_id_prop": "guid",
            "dst_id_value": "participant-guid-001",
        }

        result.single.return_value = {"edge_count": 0}
        self.assertFalse(RemoteValidator.if_edge_exist_in_db(driver, rel_dict))

        result.single.return_value = {"edge_count": 1}
        self.assertTrue(RemoteValidator.if_edge_exist_in_db(driver, rel_dict))

        result.single.return_value = {"edge_count": 2}
        with self.assertRaises(ValueError):
            RemoteValidator.if_edge_exist_in_db(driver, rel_dict)

    def test_get_node_record_in_db_removes_timestamps_and_raises_on_duplicates(self) -> None:
        session = MagicMock()
        result = MagicMock()
        session.run.return_value = result
        driver = self._build_driver_with_session(session)

        result.single.return_value = {
            "node_count": 1,
            "node": {
                "guid": "study-guid-001",
                "study_id": "phs001",
                "created": "2026-01-01T00:00:00",
                "updated": "2026-01-02T00:00:00",
            },
        }
        record = RemoteValidator.get_node_record_in_db(driver, "study-guid-001", id_prop_name="guid")
        self.assertEqual(record, {"guid": "study-guid-001", "study_id": "phs001"})

        result.single.return_value = {"node_count": 0, "node": None}
        self.assertIsNone(RemoteValidator.get_node_record_in_db(driver, "missing-guid", id_prop_name="guid"))

        result.single.return_value = {"node_count": 2, "node": {"guid": "duplicate-guid"}}
        with self.assertRaises(ValueError):
            RemoteValidator.get_node_record_in_db(driver, "duplicate-guid", id_prop_name="guid")

    def test_get_record_outgoing_edges_in_db_returns_flat_relationship_list(self) -> None:
        session = MagicMock()
        session.run.return_value = [
            {
                "src_label": "sample",
                "dst_label": "participant",
                "values": ["participant-guid-001", "participant-guid-002"],
            },
            {
                "src_label": "sample",
                "dst_label": "study",
                "values": ["study-guid-001"],
            },
            {
                "src_label": "sample",
                "dst_label": None,
                "values": [],
            },
        ]
        driver = self._build_driver_with_session(session)

        edges = RemoteValidator.get_record_outgoing_edges_in_db(
            driver,
            id_prop_value="sample-guid-001",
            id_prop_name="guid",
            node_label="sample",
        )

        self.assertEqual(
            edges,
            [
                {
                    "src_label": "sample",
                    "src_id_prop": "guid",
                    "src_id_value": "sample-guid-001",
                    "dst_label": "participant",
                    "dst_id_prop": "guid",
                    "dst_id_value": "participant-guid-001",
                },
                {
                    "src_label": "sample",
                    "src_id_prop": "guid",
                    "src_id_value": "sample-guid-001",
                    "dst_label": "participant",
                    "dst_id_prop": "guid",
                    "dst_id_value": "participant-guid-002",
                },
                {
                    "src_label": "sample",
                    "src_id_prop": "guid",
                    "src_id_value": "sample-guid-001",
                    "dst_label": "study",
                    "dst_id_prop": "guid",
                    "dst_id_value": "study-guid-001",
                },
            ],
        )

    def test_build_tsv_id_set_and_if_node_id_in_tsv_list(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            file_a = Path(tmp_dir) / "participant_a.tsv"
            file_b = Path(tmp_dir) / "participant_b.tsv"
            file_a.write_text(
                "type\tguid\tparticipant_id\n"
                "participant\tparticipant-guid-001\tP001\n",
                encoding="utf-8",
            )
            file_b.write_text(
                "type\tguid\tparticipant_id\n"
                "participant\tparticipant-guid-002\tP002\n",
                encoding="utf-8",
            )

            tsv_id_set = RemoteValidator.build_tsv_id_set(
                [str(file_a), str(file_b)], id_field="guid"
            )
            self.assertTrue(
                RemoteValidator.if_node_id_in_tsv_list(
                    tsv_id_set=tsv_id_set,
                    id_value="participant-guid-002",
                )
            )
            self.assertFalse(
                RemoteValidator.if_node_id_in_tsv_list(
                    tsv_id_set=tsv_id_set,
                    id_value="participant-guid-999",
                )
            )

    def test_if_file_records_exist_in_db_maps_rows_and_empty_values(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tsv_path = Path(tmp_dir) / "records.tsv"
            tsv_path.write_text(
                "type\tguid\n"
                "participant\tguid-001\n"
                "participant\t\n"
                "participant\tguid-002\n",
                encoding="utf-8",
            )

            session = MagicMock()
            session.run.side_effect = [
                [
                    {"id_val": "guid-001", "node_count": 1},
                    {"id_val": "guid-002", "node_count": 0},
                ]
            ]
            driver = self._build_driver_with_session(session)

            exists_by_row = RemoteValidator.if_file_records_exist_in_db(
                driver=driver,
                file_path=str(tsv_path),
                id_prop_name="guid",
                node_label="participant",
                batch_size=10000,
            )

        self.assertEqual(exists_by_row, {2: True, 3: False, 4: False})

    def test_if_file_records_exist_in_db_raises_on_duplicate_db_nodes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tsv_path = Path(tmp_dir) / "records.tsv"
            tsv_path.write_text(
                "type\tguid\n"
                "participant\tguid-dup\n",
                encoding="utf-8",
            )

            session = MagicMock()
            session.run.side_effect = [[{"id_val": "guid-dup", "node_count": 2}]]
            driver = self._build_driver_with_session(session)

            with self.assertRaisesRegex(ValueError, "Expected at most 1 unique node"):
                RemoteValidator.if_file_records_exist_in_db(
                    driver=driver,
                    file_path=str(tsv_path),
                    id_prop_name="guid",
                    node_label="participant",
                )

    def test_get_file_records_in_db_maps_rows_and_removes_timestamps(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tsv_path = Path(tmp_dir) / "records.tsv"
            tsv_path.write_text(
                "type\tguid\n"
                "participant\tguid-001\n"
                "participant\t\n"
                "participant\tguid-002\n",
                encoding="utf-8",
            )

            session = MagicMock()
            session.run.side_effect = [
                [
                    {
                        "id_val": "guid-001",
                        "node_count": 1,
                        "node": {
                            "guid": "guid-001",
                            "participant_id": "P001",
                            "created": "2026-01-01T00:00:00",
                            "updated": "2026-01-02T00:00:00",
                        },
                    },
                    {"id_val": "guid-002", "node_count": 0, "node": None},
                ]
            ]
            driver = self._build_driver_with_session(session)

            records_by_row = RemoteValidator.get_file_records_in_db(
                driver=driver,
                file_path=str(tsv_path),
                id_prop_name="guid",
                node_label="participant",
                batch_size=10000,
            )

        self.assertEqual(
            records_by_row,
            {
                2: {"guid": "guid-001", "participant_id": "P001"},
                3: None,
                4: None,
            },
        )

    def test_get_file_records_in_db_raises_on_duplicate_db_nodes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tsv_path = Path(tmp_dir) / "records.tsv"
            tsv_path.write_text(
                "type\tguid\n"
                "participant\tguid-dup\n",
                encoding="utf-8",
            )

            session = MagicMock()
            session.run.side_effect = [
                [{"id_val": "guid-dup", "node_count": 2, "node": {"guid": "guid-dup"}}]
            ]
            driver = self._build_driver_with_session(session)

            with self.assertRaisesRegex(ValueError, "Expected exactly 0 or 1 node"):
                RemoteValidator.get_file_records_in_db(
                    driver=driver,
                    file_path=str(tsv_path),
                    id_prop_name="guid",
                    node_label="participant",
                )

    def test_get_file_records_outgoing_edges_in_db_maps_rows_none_empty_and_edges(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tsv_path = Path(tmp_dir) / "records.tsv"
            tsv_path.write_text(
                "type\tguid\n"
                "sample\tguid-001\n"
                "sample\tguid-002\n"
                "sample\t\n"
                "sample\tguid-003\n",
                encoding="utf-8",
            )

            session = MagicMock()
            session.run.side_effect = [
                [
                    {
                        "id_val": "guid-001",
                        "src_count": 1,
                        "node_exists": True,
                        "src_label": "sample",
                        "edge_groups": [
                            {
                                "dst_label": "participant",
                                "values": ["participant-guid-001", "participant-guid-002"],
                            },
                            {"dst_label": None, "values": []},
                        ],
                    },
                    {
                        "id_val": "guid-002",
                        "src_count": 1,
                        "node_exists": True,
                        "src_label": "sample",
                        "edge_groups": [{"dst_label": None, "values": []}],
                    },
                    {
                        "id_val": "guid-003",
                        "src_count": 0,
                        "node_exists": False,
                        "src_label": None,
                        "edge_groups": [],
                    },
                ]
            ]
            driver = self._build_driver_with_session(session)

            edges_by_row = RemoteValidator.get_file_records_outgoing_edges_in_db(
                driver=driver,
                file_path=str(tsv_path),
                id_prop_name="guid",
                node_label="sample",
                batch_size=10000,
            )

        self.assertEqual(
            edges_by_row[2],
            [
                {
                    "src_label": "sample",
                    "src_id_prop": "guid",
                    "src_id_value": "guid-001",
                    "dst_label": "participant",
                    "dst_id_prop": "guid",
                    "dst_id_value": "participant-guid-001",
                },
                {
                    "src_label": "sample",
                    "src_id_prop": "guid",
                    "src_id_value": "guid-001",
                    "dst_label": "participant",
                    "dst_id_prop": "guid",
                    "dst_id_value": "participant-guid-002",
                },
            ],
        )
        self.assertEqual(edges_by_row[3], [])
        self.assertIsNone(edges_by_row[4])
        self.assertIsNone(edges_by_row[5])

    def test_get_file_records_outgoing_edges_in_db_raises_on_duplicate_src_nodes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tsv_path = Path(tmp_dir) / "records.tsv"
            tsv_path.write_text(
                "type\tguid\n"
                "sample\tguid-dup\n",
                encoding="utf-8",
            )

            session = MagicMock()
            session.run.side_effect = [
                [
                    {
                        "id_val": "guid-dup",
                        "src_count": 2,
                        "node_exists": True,
                        "src_label": "sample",
                        "edge_groups": [],
                    }
                ]
            ]
            driver = self._build_driver_with_session(session)

            with self.assertRaisesRegex(ValueError, "Expected exactly 0 or 1 node"):
                RemoteValidator.get_file_records_outgoing_edges_in_db(
                    driver=driver,
                    file_path=str(tsv_path),
                    id_prop_name="guid",
                    node_label="sample",
                )

    def test_validate_tsv_in_db_new_mode_flags_existing_record(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tsv_path = Path(tmp_dir) / "survival.tsv"
            self._write_survival_tsv(tsv_path)
            tsv_id_set = RemoteValidator.build_tsv_id_set([str(tsv_path)], id_field="guid")

            with patch.object(RemoteValidator, "if_file_records_exist_in_db", return_value={2: True}), \
                 patch.object(RemoteValidator, "get_file_records_in_db", return_value={2: None}), \
                 patch.object(RemoteValidator, "get_file_records_outgoing_edges_in_db", return_value={2: None}), \
                 patch.object(RemoteValidator, "if_parent_nodes_exist_in_db", return_value={}):
                (
                    passed_rows,
                    failed_rows,
                    val_summary,
                    validation_results,
                ) = RemoteValidator.validate_tsv_in_db(
                    driver=MagicMock(),
                    tsv_file_path=str(tsv_path),
                    tsv_id_set=tsv_id_set,
                    mdf_instance=self.mdf_reader,
                    id_prop_name="guid",
                    validation_mode="New",
                )

        self.assertEqual(passed_rows, [])
        self.assertEqual(failed_rows, [2])
        self.assertEqual(val_summary["total_rows"], 1)
        self.assertEqual(val_summary["passed_row_count"], 0)
        self.assertEqual(val_summary["failed_row_count"], 1)
        self.assertEqual(
            val_summary["projected_changes_of_passed_rows"],
            {
                "nodes_to_create": 0,
                "nodes_to_update": 0,
                "edges_to_create": 0,
                "edges_to_delete": 0,
            },
        )
        self.assertEqual(len(validation_results), 1)
        self.assertEqual(validation_results[0]["type"], "record_already_exist_in_db")
        self.assertEqual(validation_results[0]["row"], 2)

    def test_validate_tsv_in_db_raises_for_invalid_validation_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tsv_path = Path(tmp_dir) / "survival.tsv"
            self._write_survival_tsv(tsv_path)
            tsv_id_set = RemoteValidator.build_tsv_id_set([str(tsv_path)], id_field="guid")

            with patch.object(RemoteValidator, "if_file_records_exist_in_db", return_value={2: False}), \
                 patch.object(RemoteValidator, "get_file_records_in_db", return_value={2: None}), \
                 patch.object(RemoteValidator, "get_file_records_outgoing_edges_in_db", return_value={2: None}), \
                 patch.object(RemoteValidator, "if_parent_nodes_exist_in_db", return_value={}):
                with self.assertRaisesRegex(ValueError, "Invalid validation_mode"):
                    RemoteValidator.validate_tsv_in_db(
                        driver=MagicMock(),
                        tsv_file_path=str(tsv_path),
                        tsv_id_set=tsv_id_set,
                        mdf_instance=self.mdf_reader,
                        id_prop_name="guid",
                        validation_mode="BadMode",
                    )

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


class TestDatabaseValidator(unittest.TestCase):
    MODEL_URL = PROJECT_ROOT / "tests" / "test_files" / "ccdi-dcc-model-test.yml"
    PROPS_URL = PROJECT_ROOT / "tests" / "test_files" / "ccdi-dcc-model-props-test.yml"

    @classmethod
    def setUpClass(cls) -> None:
        try:
            cls.mdf_reader = MDFReader(
                str(cls.MODEL_URL),
                str(cls.PROPS_URL),
                handle="ccdi_dcc",
            )
        except Exception as error:
            raise unittest.SkipTest(
                f"Could not initialize model from local fixture files: {error}"
            ) from error

    def setUp(self) -> None:
        self.validator = DatabaseValidator(self.mdf_reader)

    @staticmethod
    def _build_driver_with_session(session: MagicMock) -> MagicMock:
        driver = MagicMock()
        session_cm = MagicMock()
        session_cm.__enter__.return_value = session
        session_cm.__exit__.return_value = None
        driver.session.return_value = session_cm
        return driver

    def test_list_all_properties_preserves_each_property_type(self) -> None:
        self.validator.model = SimpleNamespace(
            nodes={
                "participant": SimpleNamespace(
                    props={
                        "name": SimpleNamespace(
                            value_domain="string", item_domain=None
                        ),
                        "status": SimpleNamespace(
                            value_domain="value_set", item_domain=None
                        ),
                        "tags": SimpleNamespace(
                            value_domain="list", item_domain="value_set"
                        ),
                        "age": SimpleNamespace(
                            value_domain="number", item_domain=None
                        ),
                    }
                ),
                "sample": SimpleNamespace(
                    props={
                        "measurements": SimpleNamespace(
                            value_domain="list", item_domain="integer"
                        )
                    }
                ),
            }
        )

        properties = self.validator.list_all_properties()

        self.assertEqual(
            properties,
            [
                {"node": "participant", "property": "name", "type": "string"},
                {"node": "participant", "property": "status", "type": "Enum"},
                {
                    "node": "participant",
                    "property": "tags",
                    "type": "List[Enum]",
                },
                {"node": "participant", "property": "age", "type": "number"},
                {
                    "node": "sample",
                    "property": "measurements",
                    "type": "List[integer]",
                },
            ],
        )

    def test_prop_db_query_returns_total_and_filled_counts(self) -> None:
        tx = MagicMock()
        tx.run.return_value.single.return_value = {"total": 5, "filled": 3}

        results = DatabaseValidator.prop_db_query(tx, "participant", "age")

        self.assertEqual(results, {"total": 5, "filled": 3})
        query = tx.run.call_args.args[0]
        self.assertIn("MATCH (n:participant)", query)
        self.assertIn("COUNT(n.age) AS filled", query)

    def test_props_db_evaluation_calculates_missing_percentages(self) -> None:
        session = MagicMock()
        driver = self._build_driver_with_session(session)
        properties = [
            {"node": "participant", "property": "age", "type": "number"},
            {"node": "sample", "property": "name", "type": "string"},
        ]
        with patch.object(
            self.validator, "list_all_properties", return_value=properties
        ), patch.object(
            self.validator,
            "prop_db_query",
            side_effect=[
                {"total": 4, "filled": 3},
                {"total": 0, "filled": 0},
            ],
        ) as query:
            results = self.validator.props_db_evaluation(driver)

        self.assertEqual(
            results,
            [
                {
                    "node": "participant",
                    "property": "age",
                    "type": "number",
                    "total": 4,
                    "filled": 3,
                    "missing_percentage": 25,
                },
                {
                    "node": "sample",
                    "property": "name",
                    "type": "string",
                    "total": 0,
                    "filled": 0,
                    "missing_percentage": 0,
                },
            ],
        )
        self.assertEqual(query.call_count, 2)
        self.assertEqual(session.begin_transaction.call_count, 2)

    def test_val_results_simplified_groups_unique_inputs_and_missing_errors(self) -> None:
        validation_results = {
            "participant": {
                "flagged_nodes": [
                    {
                        "validation_result": {
                            "warnings": [
                                {
                                    "type": "enum",
                                    "loc": ["status"],
                                    "input": "Unknown",
                                }
                            ],
                            "errors": [
                                {"type": "missing", "loc": ["age"]},
                                {
                                    "type": "value_error",
                                    "loc": ["age"],
                                    "input": "invalid",
                                },
                            ],
                        }
                    },
                    {
                        "validation_result": {
                            "warnings": [
                                {
                                    "type": "enum",
                                    "loc": ["status"],
                                    "input": "Unknown",
                                }
                            ],
                            "errors": [],
                        }
                    },
                ]
            },
            "sample": {"flagged_nodes": []},
        }
        with tempfile.TemporaryDirectory() as tmp_dir:
            filepath = Path(tmp_dir) / "validation.json"
            filepath.write_text(json.dumps(validation_results), encoding="utf-8")

            simplified = DatabaseValidator.val_results_simplified(str(filepath))

        self.assertEqual(
            simplified,
            [
                {
                    "node_type": "participant",
                    "level": "error",
                    "type": "missing",
                    "loc": "age",
                    "violated_inputs_uniq": [""],
                    "n_unique": 1,
                    "n_total": 1,
                },
                {
                    "node_type": "participant",
                    "level": "error",
                    "type": "value_error",
                    "loc": "age",
                    "violated_inputs_uniq": ["invalid"],
                    "n_unique": 1,
                    "n_total": 1,
                },
                {
                    "node_type": "participant",
                    "level": "warning",
                    "type": "enum",
                    "loc": "status",
                    "violated_inputs_uniq": ["Unknown"],
                    "n_unique": 1,
                    "n_total": 2,
                },
            ],
        )

    def test_list_all_enum_properties_includes_scalar_and_list_enums(self) -> None:
        self.validator.model = SimpleNamespace(
            nodes={
                "participant": SimpleNamespace(
                    props={
                        "status": SimpleNamespace(
                            value_domain="value_set",
                            item_domain=None,
                            values=["Active", "Unknown"],
                        ),
                        "tags": SimpleNamespace(
                            value_domain="list",
                            item_domain="value_set",
                            values=["Known", "Not Reported"],
                        ),
                        "age": SimpleNamespace(
                            value_domain="number",
                            item_domain=None,
                            values=None,
                        ),
                    }
                )
            }
        )

        enum_properties = self.validator.list_all_enum_properties()

        self.assertEqual(
            enum_properties,
            [
                {
                    "node": "participant",
                    "property": "status",
                    "type": "Enum",
                    "value_set": ["Active", "Unknown"],
                },
                {
                    "node": "participant",
                    "property": "tags",
                    "type": "List[Enum]",
                    "value_set": ["Known", "Not Reported"],
                },
            ],
        )

    def test_enum_prop_db_query_counts_scalar_and_list_placeholders(self) -> None:
        for is_list, expected_condition in (
            (False, "toLower(toString(n.`status`)) = toLower($ph0)"),
            (
                True,
                "any(v IN n.`status` WHERE toLower(toString(v)) = toLower($ph0))",
            ),
        ):
            with self.subTest(is_list=is_list):
                tx = MagicMock()
                tx.run.return_value.single.return_value = {
                    "total": 8,
                    "filled": 6,
                    "ph0": 2,
                    "ph1": 1,
                }

                results = DatabaseValidator.enum_prop_db_query(
                    tx,
                    "participant",
                    "status",
                    is_list,
                    placeholders=["Unknown", "Not Reported"],
                )

                self.assertEqual(
                    results,
                    {
                        "total": 8,
                        "filled": 6,
                        "placeholders": {"Unknown": 2, "Not Reported": 1},
                    },
                )
                query = tx.run.call_args.args[0]
                self.assertIn(expected_condition, query)
                self.assertIn("AS ph0", query)
                self.assertIn("AS ph1", query)
                self.assertEqual(
                    tx.run.call_args.kwargs,
                    {"ph0": "Unknown", "ph1": "Not Reported"},
                )

    def test_enum_props_db_evaluation_skips_properties_without_placeholders(self) -> None:
        session = MagicMock()
        driver = self._build_driver_with_session(session)
        enum_properties = [
            {
                "node": "participant",
                "property": "status",
                "type": "Enum",
                "value_set": ["Active", "Unknown"],
            },
            {
                "node": "participant",
                "property": "tags",
                "type": "List[Enum]",
                "value_set": ["Known", "Unknown"],
            },
            {
                "node": "study",
                "property": "phase",
                "type": "Enum",
                "value_set": ["Active", "Completed"],
            },
        ]
        query_results = [
            {"total": 3, "filled": 2, "placeholders": {"Unknown": 1}},
            {"total": 4, "filled": 3, "placeholders": {"Unknown": 2}},
        ]
        with patch.object(
            self.validator, "list_all_enum_properties", return_value=enum_properties
        ), patch.object(
            self.validator,
            "enum_prop_db_query",
            side_effect=query_results,
        ) as query:
            results = self.validator.enum_props_db_evaluation(
                driver, placeholders=["Unknown"]
            )

        self.assertEqual(
            results,
            [
                {
                    "node": "participant",
                    "property": "status",
                    "type": "Enum",
                    **query_results[0],
                },
                {
                    "node": "participant",
                    "property": "tags",
                    "type": "List[Enum]",
                    **query_results[1],
                },
            ],
        )
        self.assertEqual(query.call_count, 2)
        self.assertEqual(query.call_args_list[0].args[1:4], ("participant", "status", False))
        self.assertEqual(query.call_args_list[1].args[1:4], ("participant", "tags", True))
        self.assertEqual(session.begin_transaction.call_count, 2)

    def test_list_all_num_properties_includes_scalar_and_list_numbers(self) -> None:
        self.validator.model = SimpleNamespace(
            nodes={
                "participant": SimpleNamespace(
                    props={
                        "age": SimpleNamespace(
                            value_domain="number", item_domain=None
                        ),
                        "count": SimpleNamespace(
                            value_domain="integer", item_domain=None
                        ),
                        "measurements": SimpleNamespace(
                            value_domain="list", item_domain="number"
                        ),
                        "ranks": SimpleNamespace(
                            value_domain="list", item_domain="integer"
                        ),
                        "tags": SimpleNamespace(
                            value_domain="list", item_domain="string"
                        ),
                    }
                )
            }
        )

        numeric_properties = self.validator.list_all_num_properties()

        self.assertEqual(
            numeric_properties,
            [
                {"node": "participant", "property": "age", "type": "number"},
                {"node": "participant", "property": "count", "type": "integer"},
                {
                    "node": "participant",
                    "property": "measurements",
                    "type": "List[number]",
                },
                {
                    "node": "participant",
                    "property": "ranks",
                    "type": "List[integer]",
                },
            ],
        )

    def test_numeric_props_db_query_counts_sentinel_values(self) -> None:
        tx = MagicMock()
        tx.run.return_value.single.return_value = {
            "total": 10,
            "filled": 7,
            "sentinel": 2,
        }

        results = DatabaseValidator.numeric_props_db_query(
            tx, "participant", "age", sentinel=-1
        )

        self.assertEqual(results, {"total": 10, "filled": 7, "sentinel": 2})
        query = tx.run.call_args.args[0]
        self.assertIn("count(n.age) AS filled", query)
        self.assertIn("n.age = $sentinel", query)
        self.assertIn("n.age = [$sentinel]", query)
        self.assertEqual(tx.run.call_args.kwargs, {"sentinel": -1})

    def test_numeric_props_db_evaluation_queries_all_numeric_properties(self) -> None:
        session = MagicMock()
        driver = self._build_driver_with_session(session)
        numeric_properties = [
            {"node": "participant", "property": "age", "type": "number"},
            {"node": "study", "property": "year", "type": "integer"},
        ]
        query_results = [
            {"total": 3, "filled": 2, "sentinel": 1},
            {"total": 2, "filled": 2, "sentinel": 0},
        ]
        with patch.object(
            self.validator, "list_all_num_properties", return_value=numeric_properties
        ), patch.object(
            self.validator,
            "numeric_props_db_query",
            side_effect=query_results,
        ) as query:
            results = self.validator.numeric_props_db_evaluation(driver, sentinel=-1)

        self.assertEqual(
            results,
            [
                {"node": "participant", "property": "age", **query_results[0]},
                {"node": "study", "property": "year", **query_results[1]},
            ],
        )
        self.assertEqual(query.call_count, 2)
        self.assertEqual(query.call_args_list[0].args[1:3], ("participant", "age"))
        self.assertEqual(query.call_args_list[1].args[1:3], ("study", "year"))
        self.assertEqual(query.call_args_list[0].args[3], -1)
        self.assertEqual(session.begin_transaction.call_count, 2)

    def test_list_all_labels_returns_unique_labels(self) -> None:
        session = MagicMock()
        session.run.return_value = [
            {"labels": ["participant", "data_node"]},
            {"labels": ["sample", "data_node"]},
        ]
        driver = self._build_driver_with_session(session)

        labels = DatabaseValidator.list_all_labels(driver)

        self.assertCountEqual(labels, ["participant", "sample", "data_node"])
        session.run.assert_called_once_with(
            "MATCH (n) RETURN DISTINCT labels(n) AS labels"
        )

    def test_clean_db_node_properties_removes_timestamps_and_optional_uuid(self) -> None:
        properties = {
            "guid": "node-guid",
            "created": "2025-01-01",
            "updated": "2025-01-02",
            "participant_id": "P001",
        }

        without_model_uuid = DatabaseValidator._clean_db_node_properties(
            properties, uuid_property="guid"
        )
        uuid_in_model = DatabaseValidator._clean_db_node_properties(
            properties, uuid_property="guid", uuid_in_model=True
        )

        self.assertEqual(without_model_uuid, {"participant_id": "P001"})
        self.assertEqual(
            uuid_in_model,
            {"guid": "node-guid", "participant_id": "P001"},
        )

    def test_fetch_nodes_by_label_fetches_batches_until_empty(self) -> None:
        session = MagicMock()
        session.run.side_effect = [
            [{"n": {"guid": "g1"}}, {"n": {"guid": "g2"}}],
            [{"n": {"guid": "g3"}}],
            [],
        ]
        driver = self._build_driver_with_session(session)

        batches = list(
            DatabaseValidator.fetch_nodes_by_label(driver, "participant", batch_size=2)
        )

        self.assertEqual(
            batches,
            [[{"guid": "g1"}, {"guid": "g2"}], [{"guid": "g3"}]],
        )
        self.assertEqual(session.run.call_count, 3)
        self.assertEqual(
            [call.kwargs for call in session.run.call_args_list],
            [
                {"offset": 0, "batch_size": 2},
                {"offset": 2, "batch_size": 2},
                {"offset": 4, "batch_size": 2},
            ],
        )

    def test_validate_db_records_by_label_counts_passes_and_flags(self) -> None:
        records = [
            {
                "guid": "guid-1",
                "created": "timestamp",
                "participant_id": "P001",
            },
            {
                "guid": "guid-2",
                "updated": "timestamp",
                "participant_id": "P002",
            },
        ]
        with patch.object(
            DatabaseValidator,
            "fetch_nodes_by_label",
            return_value=iter([records]),
        ), patch.object(
            self.validator,
            "validate_one_record",
            side_effect=[
                (True, {"warnings": [{"type": "warning"}], "errors": []}),
                (False, {"warnings": [], "errors": [{"type": "invalid"}]}),
            ],
        ) as validate_one_record:
            node_count, passed_count, flagged_nodes = (
                self.validator.validate_db_records_by_label(
                    "participant", MagicMock(), uuid_property="guid"
                )
            )

        self.assertEqual((node_count, passed_count), (2, 1))
        self.assertEqual(len(flagged_nodes), 2)
        self.assertTrue(flagged_nodes[0]["valid"])
        self.assertNotIn("guid", flagged_nodes[0]["data_record"])
        self.assertNotIn("created", flagged_nodes[0]["data_record"])
        self.assertFalse(flagged_nodes[1]["valid"])
        self.assertEqual(validate_one_record.call_count, 2)

    def test_validate_db_records_aggregates_results_for_each_label(self) -> None:
        with patch.object(
            DatabaseValidator, "list_all_labels", return_value=["participant", "sample"]
        ), patch.object(
            self.validator,
            "validate_db_records_by_label",
            side_effect=[
                (3, 2, [{"label": "participant", "valid": False}]),
                (5, 5, []),
            ],
        ) as validate_by_label:
            results = self.validator.validate_db_records(
                MagicMock(), batch_size=100, uuid_property="uuid", uuid_in_model=True
            )

        self.assertEqual(
            results,
            {
                "participant": {
                    "node_count": 3,
                    "passed_count": 2,
                    "flagged_nodes": [{"label": "participant", "valid": False}],
                },
                "sample": {
                    "node_count": 5,
                    "passed_count": 5,
                    "flagged_nodes": [],
                },
            },
        )
        self.assertEqual(validate_by_label.call_count, 2)
        self.assertEqual(validate_by_label.call_args_list[0].args[0], "participant")
        self.assertEqual(validate_by_label.call_args_list[1].args[0], "sample")
        self.assertEqual(validate_by_label.call_args_list[0].args[2:], (100, "uuid", True))

    def test_get_basic_val_matrix_summarizes_node_counts(self) -> None:
        results = {
            "participant": {
                "node_count": 4,
                "passed_count": 3,
                "flagged_nodes": [
                    {"valid": True},
                    {"valid": False},
                ],
            }
        }

        matrix = DatabaseValidator.get_basic_val_matrix(results)

        self.assertEqual(matrix.loc[0, "label"], "participant")
        self.assertEqual(matrix.loc[0, "node_count"], 4)
        self.assertEqual(matrix.loc[0, "passed_count"], 3)
        self.assertEqual(matrix.loc[0, "failed_counts(with errors)"], 1)
        self.assertEqual(matrix.loc[0, "flagged_node_counts"], 2)
        self.assertEqual(matrix.loc[0, "node_with_warnings_but_valid"], 1)

    def test_validate_edges_many_to_one_formats_violations_and_checks_multiplicity(self) -> None:
        session = MagicMock()
        session.run.return_value = [
            {
                "src_key_prop": "sample-1",
                "src_uuid": "sample-guid",
                "dsts": [
                    {"key": "participant-1", "uuid": "participant-guid-1"},
                    {"key": "participant-2", "uuid": "participant-guid-2"},
                ],
                "degree": 2,
            }
        ]
        driver = self._build_driver_with_session(session)
        model_parser = MagicMock()
        model_parser.get_node_key_prop.side_effect = {
            "sample": "sample_id",
            "participant": "participant_id",
        }.__getitem__
        model_parser.get_edge_multiplicity.return_value = "many_to_one"
        model_parser.get_edge_handle.return_value = "of_participant"

        results = self.validator.validate_edges_many_to_one(
            driver, model_parser, "sample", "participant", uuid_property="uuid"
        )

        self.assertEqual(
            results,
            {
                "edge_validation_type": "many_to_one",
                "edge": {
                    "src_label": "sample",
                    "dst_label": "participant",
                    "edge_handle": "of_participant",
                    "edge_multiplicity": "many_to_one",
                },
                "violations": [
                    {
                        "sample_id": "sample-1",
                        "uuid": "sample-guid",
                        "number_of_dst": 2,
                        "dst_nodes": [
                            {
                                "participant_id": "participant-1",
                                "uuid": "participant-guid-1",
                            },
                            {
                                "participant_id": "participant-2",
                                "uuid": "participant-guid-2",
                            },
                        ],
                    }
                ],
            },
        )
        self.assertIn("count(b) AS degree", session.run.call_args.args[0])

        model_parser.get_edge_multiplicity.return_value = "one_to_many"
        with self.assertRaisesRegex(ValueError, "is not many_to_one"):
            self.validator.validate_edges_many_to_one(
                driver, model_parser, "sample", "participant"
            )

    def test_validate_edges_one_to_many_formats_violations_and_empty_results(self) -> None:
        session = MagicMock()
        session.run.return_value = [
            {
                "dst_key_prop": "participant-1",
                "dst_uuid": "participant-guid",
                "srcs": [{"key": "sample-1", "uuid": "sample-guid"}],
                "degree": 2,
            }
        ]
        driver = self._build_driver_with_session(session)
        model_parser = MagicMock()
        model_parser.get_node_key_prop.side_effect = {
            "sample": "sample_id",
            "participant": "participant_id",
        }.__getitem__
        model_parser.get_edge_multiplicity.return_value = "one_to_many"
        model_parser.get_edge_handle.return_value = "of_participant"

        results = self.validator.validate_edges_one_to_many(
            driver, model_parser, "sample", "participant", uuid_property="uuid"
        )

        self.assertEqual(
            results,
            {
                "edge_validation_type": "one_to_many",
                "edge": {
                    "src_label": "sample",
                    "dst_label": "participant",
                    "edge_handle": "of_participant",
                    "edge_multiplicity": "one_to_many",
                },
                "violations": [
                    {
                        "participant_id": "participant-1",
                        "uuid": "participant-guid",
                        "number_of_src": 2,
                        "src_nodes": [
                            {"sample_id": "sample-1", "uuid": "sample-guid"}
                        ],
                    }
                ],
            },
        )

        session.run.return_value = []
        self.assertEqual(
            self.validator.validate_edges_one_to_many(
                driver, model_parser, "sample", "participant"
            ),
            {},
        )
        model_parser.get_edge_multiplicity.return_value = "many_to_one"
        with self.assertRaisesRegex(ValueError, "is not one_to_many"):
            self.validator.validate_edges_one_to_many(
                driver, model_parser, "sample", "participant"
            )

    def test_validate_edges_one_to_one_reports_violations_on_both_sides(self) -> None:
        session = MagicMock()
        session.run.side_effect = [
            [
                {
                    "src_key_prop": "sample-1",
                    "src_uuid": "sample-guid",
                    "dsts": [{"key": "participant-1", "uuid": "participant-guid-1"}],
                    "degree": 2,
                }
            ],
            [
                {
                    "dst_key_prop": "participant-2",
                    "dst_uuid": "participant-guid-2",
                    "srcs": [{"key": "sample-2", "uuid": "sample-guid-2"}],
                    "degree": 2,
                }
            ],
        ]
        driver = self._build_driver_with_session(session)
        model_parser = MagicMock()
        model_parser.get_node_key_prop.side_effect = {
            "sample": "sample_id",
            "participant": "participant_id",
        }.__getitem__
        model_parser.get_edge_multiplicity.return_value = "one_to_one"
        model_parser.get_edge_handle.return_value = "of_participant"

        results = self.validator.validate_edges_one_to_one(
            driver, model_parser, "sample", "participant", uuid_property="uuid"
        )

        self.assertEqual(
            results,
            {
                "edge_validation_type": "one_to_one",
                "edge": {
                    "src_label": "sample",
                    "dst_label": "participant",
                    "edge_handle": "of_participant",
                    "edge_multiplicity": "one_to_one",
                },
                "violations": {
                    "src_side_violations": [
                        {
                            "sample_id": "sample-1",
                            "uuid": "sample-guid",
                            "number_of_dst": 2,
                            "dst_nodes": [
                                {
                                    "participant_id": "participant-1",
                                    "uuid": "participant-guid-1",
                                }
                            ],
                        }
                    ],
                    "dst_side_violations": [
                        {
                            "participant_id": "participant-2",
                            "uuid": "participant-guid-2",
                            "number_of_src": 2,
                            "src_nodes": [
                                {"sample_id": "sample-2", "uuid": "sample-guid-2"}
                            ],
                        }
                    ],
                },
            },
        )
        self.assertEqual(session.run.call_count, 2)

        model_parser.get_edge_multiplicity.return_value = "many_to_one"
        with self.assertRaisesRegex(ValueError, "is not one_to_one"):
            self.validator.validate_edges_one_to_one(
                driver, model_parser, "sample", "participant"
            )

    def test_validate_db_edges_multiplicity_dispatches_supported_edges(self) -> None:
        grouped_edges = {
            "many_to_many": [
                {"src": "a", "dst": "b", "handle": "ab", "multiplicity": "many_to_many"}
            ],
            "one_to_one": [
                {"src": "c", "dst": "d", "handle": "cd", "multiplicity": "one_to_one"}
            ],
            "one_to_many": [
                {"src": "e", "dst": "f", "handle": "ef", "multiplicity": "one_to_many"}
            ],
            "many_to_one": [
                {"src": "g", "dst": "h", "handle": "gh", "multiplicity": "many_to_one"}
            ],
        }
        model_parser = MagicMock()
        model_parser.group_edges_by_multiplicity.return_value = grouped_edges
        with patch.object(
            self.validator, "validate_edges_one_to_one", return_value={"violation": "1"}
        ) as one_to_one, patch.object(
            self.validator, "validate_edges_one_to_many", return_value={}
        ) as one_to_many, patch.object(
            self.validator, "validate_edges_many_to_one", return_value={"violation": "2"}
        ) as many_to_one:
            results = self.validator.validate_db_edges_multiplicity(
                MagicMock(), model_parser, uuid_property="uuid"
            )

        self.assertEqual(results, [{"violation": "1"}, {"violation": "2"}])
        one_to_one.assert_called_once_with(
            ANY, model_parser, "c", "d", "uuid"
        )
        one_to_many.assert_called_once_with(
            ANY, model_parser, "e", "f", "uuid"
        )
        many_to_one.assert_called_once_with(
            ANY, model_parser, "g", "h", "uuid"
        )

    def test_validate_db_edges_multiplicity_rejects_unknown_values(self) -> None:
        model_parser = MagicMock()
        model_parser.group_edges_by_multiplicity.return_value = {
            "unknown": [{"src": "a", "dst": "b", "handle": "ab"}]
        }

        with self.assertRaisesRegex(ValueError, "unfound multiplicities"):
            self.validator.validate_db_edges_multiplicity(MagicMock(), model_parser)
if __name__ == "__main__":
    unittest.main()
