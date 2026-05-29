import sys
import unittest
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
	sys.path.insert(0, str(PROJECT_ROOT))

from meval.loader import Loader


class FakeModelParser:
	def get_node_props_if_list_type(self, node_name: str) -> list[str]:
		if node_name == "sample":
			return ["tags"]
		return []

	def get_prop_type(self, node_name: str, prop_name: str) -> str:
		if node_name == "sample" and prop_name == "count":
			return "integer"
		return "string"

	def get_edge_handle(self, edge_src: str, edge_dst: str) -> str:
		if edge_src == "sample" and edge_dst == "participant":
			return "of_participant"
		return "related_to"


class TestLoader(unittest.TestCase):
	@staticmethod
	def _build_loader_with_mock_session(session: MagicMock) -> Loader:
		driver = MagicMock()
		session_cm = MagicMock()
		session_cm.__enter__.return_value = session
		session_cm.__exit__.return_value = None
		driver.session.return_value = session_cm
		return Loader(driver=driver)

	def test_chunks(self) -> None:
		data = [1, 2, 3, 4, 5]
		result = list(Loader.chunks(data, size=2))
		self.assertEqual(result, [[1, 2], [3, 4], [5]])

	def test_generate_chunk_records_cleans_and_converts_types(self) -> None:
		parser = FakeModelParser()
		chunk = pd.DataFrame(
			{
				"type": ["sample", "sample"],
				"guid": ["s1", "s2"],
				"name": ["alpha", "beta"],
				"tags": ["a ; b", "single"],
				"count": [7, 8],
				"participant.guid": ["p1", "p2"],
				"subgraph": ["sg1", "sg1"],
				"notes": [float("nan"), "ok"],
			}
		)

		chunk_type, records = Loader.generate_chunk_records(
			chunk=chunk,
			model_parser=parser,
			subgraph_col="subgraph",
			delimiter=";",
		)

		self.assertEqual(chunk_type, "sample")
		self.assertEqual(len(records), 2)
		self.assertNotIn("participant.guid", records[0])
		self.assertNotIn("subgraph", records[0])
		self.assertEqual(records[0]["tags"], ["a", "b"])
		self.assertEqual(records[1]["tags"], ["single"])
		self.assertIsInstance(records[0]["count"], int)
		self.assertNotIn("notes", records[0])
		self.assertEqual(records[1]["notes"], "ok")

	def test_generate_chunk_relationships_handles_one_to_many(self) -> None:
		parser = FakeModelParser()
		chunk = pd.DataFrame(
			{
				"type": ["sample", "sample"],
				"guid": ["s1", "s2"],
				"participant.guid": ["p1;p2", "p3"],
				"name": ["x", "y"],
			}
		)

		edges = Loader.generate_chunk_relationships(
			chunk=chunk,
			model_parser=parser,
			id_field="guid",
			delimiter=";",
		)

		self.assertEqual(len(edges), 3)
		self.assertEqual(edges[0]["src_label"], "sample")
		self.assertEqual(edges[0]["dst_label"], "participant")
		self.assertEqual(edges[0]["handle"], "of_participant")
		self.assertSetEqual(
			{edge["dst_match"] for edge in edges},
			{"p1", "p2", "p3"},
		)

	def test_remove_chunk_duplicates_reports_file_rows(self) -> None:
		chunk = pd.DataFrame(
			{
				"guid": ["g1", "g2", "g1"],
				"type": ["sample", "sample", "sample"],
			}
		)

		updated_chunk, remain_rows, removed_rows = Loader.remove_chunk_duplicates(
			chunk=chunk,
			id_field="guid",
			data_start_offset=2,
			logger=None,
		)

		self.assertEqual(updated_chunk["guid"].tolist(), ["g2", "g1"])
		self.assertEqual(removed_rows, [2])
		self.assertEqual(remain_rows, [3, 4])

	def test_read_tsv_at_row_number(self) -> None:
		with tempfile.TemporaryDirectory() as tmp_dir:
			tsv_path = Path(tmp_dir) / "sample.tsv"
			tsv_path.write_text(
				"type\tguid\tname\n"
				"sample\ts1\tAlpha\n"
				"sample\ts2\tBeta\n",
				encoding="utf-8",
			)

			row2 = Loader.read_tsv_at_row_number(str(tsv_path), row_number=2)
			row3 = Loader.read_tsv_at_row_number(str(tsv_path), row_number=3)

			self.assertEqual(row2["guid"], "s1")
			self.assertEqual(row3["name"], "Beta")

	def test_generate_del_rel_list_of_a_record(self) -> None:
		record = {
			"type": "sample",
			"guid": "s1",
			"participant.guid": "p1;p2",
			"study.guid": "st1",
		}

		rels = Loader.generate_del_rel_list_of_a_record(
			record_dict=record,
			id_field="guid",
			delimiter=";",
		)

		self.assertEqual(len(rels), 3)
		self.assertSetEqual(
			{(r["dst_label"], r["dst_match"]) for r in rels},
			{("participant", "p1"), ("participant", "p2"), ("study", "st1")},
		)

	def test_turn_remain_row_list_to_dict(self) -> None:
		chunk = pd.DataFrame(
			{
				"guid": ["g1", "g2"],
				"type": ["sample", "sample"],
			}
		)
		out = Loader.turn_remain_row_list_to_dict(
			chunk=chunk,
			file_path="folder/sample.tsv",
			remain_row_list=[10, 11],
			id_field="guid",
		)

		self.assertEqual(out["g1"]["row_number"], 10)
		self.assertEqual(out["g2"]["file_path"], "folder/sample.tsv")

	def test_find_nodes_without_path_to_root(self) -> None:
		class DummyNode:
			def __init__(self, node_id: int, labels: set[str], props: dict) -> None:
				self.id = node_id
				self.labels = labels
				self._props = props

			def keys(self):
				return self._props.keys()

			def get(self, key, default=None):
				return self._props.get(key, default)

		session = MagicMock()
		node = DummyNode(
			node_id=101,
			labels={"sample"},
			props={"guid": "g-101", "name": "S1"},
		)
		session.run.return_value = [{"n": node}]
		loader = self._build_loader_with_mock_session(session)

		rows = list(loader.find_nodes_without_path_to_root(root_node_label="study"))

		self.assertEqual(len(rows), 1)
		self.assertEqual(rows[0]["db_internal_id"], 101)
		self.assertEqual(rows[0]["type"], "sample")
		self.assertEqual(rows[0]["properties"]["guid"], "g-101")
		session.run.assert_called_once()
		self.assertIn(":study", session.run.call_args.args[0])

	def test_delete_nodes_by_internal_id_batches_and_sums_deleted(self) -> None:
		session = MagicMock()

		def run_side_effect(_query, node_ids):
			result = MagicMock()
			result.single.return_value = {"deleted_nodes": len(node_ids)}
			return result

		session.run.side_effect = run_side_effect
		loader = self._build_loader_with_mock_session(session)

		deleted = loader.delete_nodes_by_internal_id(
			identifier_list=["1", "2", "3"],
			batch_size=2,
		)

		self.assertEqual(deleted, 3)
		self.assertEqual(session.run.call_count, 2)
		first_call = session.run.call_args_list[0]
		second_call = session.run.call_args_list[1]
		self.assertEqual(first_call.kwargs["node_ids"], [1, 2])
		self.assertEqual(second_call.kwargs["node_ids"], [3])

	def test_delete_nodes_by_internal_id_invalid_identifier_raises(self) -> None:
		session = MagicMock()
		loader = self._build_loader_with_mock_session(session)

		with self.assertRaises(ValueError):
			loader.delete_nodes_by_internal_id(identifier_list=["1", "abc"], batch_size=2)

		session.run.assert_not_called()

	def test_delete_nodes_by_prop_value_batches_and_sums_deleted(self) -> None:
		session = MagicMock()

		def run_side_effect(query, property_value):
			self.assertIn("WHERE n.guid = prop_val", query)
			result = MagicMock()
			result.single.return_value = {"deleted_nodes": len(property_value)}
			return result

		session.run.side_effect = run_side_effect
		loader = self._build_loader_with_mock_session(session)

		deleted = loader.delete_nodes_by_prop_value(
			identifier_list=["g1", "g2", "g3"],
			property_name="guid",
			batch_size=2,
		)

		self.assertEqual(deleted, 3)
		self.assertEqual(session.run.call_count, 2)
		first_call = session.run.call_args_list[0]
		second_call = session.run.call_args_list[1]
		self.assertEqual(first_call.kwargs["property_value"], ["g1", "g2"])
		self.assertEqual(second_call.kwargs["property_value"], ["g3"])


if __name__ == "__main__":
	unittest.main()
