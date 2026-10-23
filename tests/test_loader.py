import sys
import unittest
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

import pandas as pd
from neo4j.exceptions import ClientError

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
	sys.path.insert(0, str(PROJECT_ROOT))

from meval.loader import Loader


class FakeModelParser:
	def __init__(self) -> None:
		self.model = SimpleNamespace(
			nodes={
				"sample": SimpleNamespace(
					props={
						"tags": SimpleNamespace(item_domain="string"),
					}
				)
			}
		)

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

	def get_node_props_list(self, node_name: str) -> list[str]:
		if node_name == "sample":
			return ["guid", "name", "tag", "count"]
		return []


class TestLoader(unittest.TestCase):
	@staticmethod
	def _build_loader_with_mock_session(session: MagicMock) -> Loader:
		driver = MagicMock()
		session_cm = MagicMock()
		session_cm.__enter__.return_value = session
		session_cm.__exit__.return_value = None
		driver.session.return_value = session_cm
		return Loader(driver=driver)

	class _FakeGraphNode(dict):
		def __init__(self, labels: set[str], properties: dict[str, str]) -> None:
			super().__init__(properties)
			self.labels = labels

	def test_close_calls_driver_close(self) -> None:
		driver = MagicMock()
		loader = Loader(driver=driver)

		self.assertIsNone(loader.close())

		driver.close.assert_called_once_with()
		driver.session.assert_not_called()

	def test_close_propagates_driver_error(self) -> None:
		driver = MagicMock()
		driver.close.side_effect = RuntimeError("Driver close failed")
		loader = Loader(driver=driver)

		with self.assertRaisesRegex(RuntimeError, "Driver close failed"):
			loader.close()

		driver.close.assert_called_once_with()

	def test_check_encoding_returns_utf8_for_ascii_and_unicode_files(self) -> None:
		for content in ("type\tname\nsample\tAlpha\n", "type\tname\nsample\tcaf\u00e9\n"):
			with self.subTest(content=content):
				with tempfile.TemporaryDirectory() as tmp_dir:
					file_path = Path(tmp_dir) / "sample.tsv"
					file_path.write_text(content, encoding="utf-8")

					self.assertEqual(Loader.check_encoding(str(file_path)), "utf-8")

	def test_check_encoding_returns_cp1252_for_non_utf8_file(self) -> None:
		with tempfile.TemporaryDirectory() as tmp_dir:
			file_path = Path(tmp_dir) / "sample.tsv"
			file_path.write_text("type\tname\nsample\tcaf\u00e9\n", encoding="cp1252")

			self.assertEqual(Loader.check_encoding(str(file_path)), "cp1252")

	def test_check_encoding_returns_utf8_for_empty_file(self) -> None:
		with tempfile.TemporaryDirectory() as tmp_dir:
			file_path = Path(tmp_dir) / "empty.tsv"
			file_path.write_text("", encoding="utf-8")

			self.assertEqual(Loader.check_encoding(str(file_path)), "utf-8")

	def test_check_encoding_propagates_missing_file_error(self) -> None:
		with tempfile.TemporaryDirectory() as tmp_dir:
			file_path = Path(tmp_dir) / "missing.tsv"

			with self.assertRaises(FileNotFoundError):
				Loader.check_encoding(str(file_path))

	def test_chunks(self) -> None:
		data = [1, 2, 3, 4, 5]
		result = list(Loader.chunks(data, size=2))
		self.assertEqual(result, [[1, 2], [3, 4], [5]])

	def test_read_file_in_chunks_preserves_values_as_strings(self) -> None:
		with tempfile.TemporaryDirectory() as tmp_dir:
			tsv_path = Path(tmp_dir) / "sample.tsv"
			tsv_path.write_text(
				"type\tguid\tcount\n"
				"sample\ts1\t7\n"
				"sample\ts2\t8\n",
				encoding="utf-8",
			)

			chunks = list(
				Loader.read_file_in_chunks(
					file_path=str(tsv_path), chunk_size=1
				)
			)

		self.assertEqual(len(chunks), 2)
		self.assertEqual(chunks[0]["count"].tolist(), ["7"])
		self.assertEqual(chunks[1]["count"].tolist(), ["8"])
		self.assertEqual(chunks[0]["count"].map(type).tolist(), [str])

	def test_generate_chunk_records_cleans_and_converts_types(self) -> None:
		parser = FakeModelParser()
		chunk = pd.DataFrame(
			{
				"type": ["sample", "sample"],
				"guid": ["s1", "s2"],
				"name": ["alpha", "beta"],
				"tags": ["a ; b", "single"],
				"count": ["7", "8"],
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
		label_result = MagicMock()
		label_result.__iter__.return_value = iter([{"labels": ["sample"]}])

		def run_side_effect(query, **kwargs):
			if "RETURN DISTINCT labels(n)" in query:
				return label_result

			self.assertIn("MATCH (n:sample {guid: pv})", query)
			result = MagicMock()
			result.single.return_value = {
				"deleted_nodes": len(kwargs["property_values"])
			}
			return result

		session.run.side_effect = run_side_effect
		loader = self._build_loader_with_mock_session(session)

		deleted = loader.delete_nodes_by_prop_value(
			identifier_list=["g1", "g2", "g3"],
			property_name="guid",
			batch_size=2,
		)

		self.assertEqual(deleted, 3)
		self.assertEqual(session.run.call_count, 3)
		first_delete_call = session.run.call_args_list[1]
		second_delete_call = session.run.call_args_list[2]
		self.assertEqual(first_delete_call.kwargs["property_values"], ["g1", "g2"])
		self.assertEqual(second_delete_call.kwargs["property_values"], ["g3"])

	def test_check_unique_node_returns_pass_for_exactly_one_match(self) -> None:
		session = MagicMock()
		node = self._FakeGraphNode(labels={"sample"}, properties={"guid": "g1", "name": "S1"})
		result = MagicMock()
		result.single.return_value = {"nodes": [node]}
		session.run.return_value = result
		loader = self._build_loader_with_mock_session(session)

		is_unique, details = loader.check_unique_node(property_value="g1", property_name="guid")

		self.assertTrue(is_unique)
		self.assertEqual(details["check_result"], "Pass")
		self.assertEqual(len(details["matched_node(s)"]), 1)
		self.assertEqual(details["matched_node(s)"][0]["properties"]["guid"], "g1")

	def test_check_unique_node_returns_fail_for_no_match(self) -> None:
		session = MagicMock()
		result = MagicMock()
		result.single.return_value = {"nodes": []}
		session.run.return_value = result
		loader = self._build_loader_with_mock_session(session)

		is_unique, details = loader.check_unique_node(property_value="missing-guid", property_name="guid")

		self.assertFalse(is_unique)
		self.assertEqual(details["check_result"], "Fail")
		self.assertIn("No node found", details["message"])
		self.assertEqual(details["matched_node(s)"], [])

	def test_check_unique_node_returns_fail_for_multiple_matches(self) -> None:
		session = MagicMock()
		node_a = self._FakeGraphNode(labels={"sample"}, properties={"guid": "dup", "name": "A"})
		node_b = self._FakeGraphNode(labels={"participant"}, properties={"guid": "dup", "name": "B"})
		result = MagicMock()
		result.single.return_value = {"nodes": [node_a, node_b]}
		session.run.return_value = result
		loader = self._build_loader_with_mock_session(session)

		is_unique, details = loader.check_unique_node(property_value="dup", property_name="guid")

		self.assertFalse(is_unique)
		self.assertEqual(details["check_result"], "Fail")
		self.assertIn("Multiple nodes found", details["message"])
		self.assertEqual(len(details["matched_node(s)"]), 2)

	def test_find_upstream_nodes_returns_collected_nodes(self) -> None:
		session = MagicMock()
		node_a = self._FakeGraphNode(labels={"participant"}, properties={"guid": "p1"})
		node_b = self._FakeGraphNode(labels={"study"}, properties={"guid": "st1"})
		result = MagicMock()
		result.single.return_value = {"upstream_nodes": [node_a, node_b]}
		session.run.return_value = result
		loader = self._build_loader_with_mock_session(session)

		nodes = loader.find_upstream_nodes(property_value="target-guid", property_name="guid")

		self.assertEqual(len(nodes), 2)
		self.assertSetEqual({n["properties"]["guid"] for n in nodes}, {"p1", "st1"})
		self.assertSetEqual({n["labels"][0] for n in nodes}, {"participant", "study"})

	def test_if_alternative_path_to_root_returns_true_when_paths_exist(self) -> None:
		session = MagicMock()
		result = MagicMock()
		result.single.return_value = {"alternative_paths_count": 2}
		session.run.return_value = result
		loader = self._build_loader_with_mock_session(session)

		has_path = loader.if_alternative_path_to_root(
			property_name="guid",
			target_property_value="target-guid",
			node_to_avoid_property_value="avoid-guid",
			root_label="study",
		)

		self.assertTrue(has_path)

	def test_if_alternative_path_to_root_returns_false_when_no_paths_exist(self) -> None:
		session = MagicMock()
		result = MagicMock()
		result.single.return_value = {"alternative_paths_count": 0}
		session.run.return_value = result
		loader = self._build_loader_with_mock_session(session)

		has_path = loader.if_alternative_path_to_root(
			property_name="guid",
			target_property_value="target-guid",
			node_to_avoid_property_value="avoid-guid",
			root_label="study",
		)

		self.assertFalse(has_path)

	def test_list_all_labels_collects_distinct_labels(self) -> None:
		session = MagicMock()
		result = MagicMock()
		result.__iter__.return_value = iter([
			{"labels": ["sample", "participant"]},
			{"labels": ["study"]},
			{"labels": ["participant"]},
		])
		session.run.return_value = result
		loader = self._build_loader_with_mock_session(session)

		labels = loader._list_all_labels()

		self.assertCountEqual(labels, ["sample", "participant", "study"])

	def test_list_index_returns_only_label_property_indexes(self) -> None:
		session = MagicMock()
		session.run.return_value = [
			{"index type": "label+property", "label": "sample", "property": ["guid"]},
			{"index type": "label", "label": "study", "property": []},
			{"index type": "label+property", "label": "participant", "property": ["id"]},
		]
		loader = self._build_loader_with_mock_session(session)

		indexes = loader._list_index()

		self.assertEqual(indexes, [
			{"label": "sample", "property": "guid"},
			{"label": "participant", "property": "id"},
		])
		session.run.assert_called_once_with("SHOW INDEXES;")

	def test_list_index_uses_fallback_after_client_error(self) -> None:
		session = MagicMock()
		session.run.side_effect = [
			ClientError("SHOW INDEXES is not supported"),
			[{"index type": "label+property", "label": "sample", "property": ["guid"]}],
		]
		loader = self._build_loader_with_mock_session(session)

		indexes = loader._list_index()

		self.assertEqual(indexes, [{"label": "sample", "property": "guid"}])
		self.assertEqual(session.run.call_args_list, [
			call("SHOW INDEXES;"),
			call("SHOW INDEX INFO;"),
		])

	def test_list_index_returns_empty_list_when_no_indexes_exist(self) -> None:
		session = MagicMock()
		session.run.return_value = []
		loader = self._build_loader_with_mock_session(session)

		self.assertEqual(loader._list_index(), [])
		session.run.assert_called_once_with("SHOW INDEXES;")

	def test_create_index_creates_only_missing_indexes(self) -> None:
		session = MagicMock()
		loader = self._build_loader_with_mock_session(session)
		model_parser = MagicMock()
		model_parser.get_node_list.return_value = ["sample", "participant", "study"]
		existing_indexes = [
			{"label": "sample", "property": "guid"},
			{"label": "participant", "property": "id"},
		]
		with patch.object(loader, "_list_index", return_value=existing_indexes) as list_index:
			indexes = loader.create_index(model_parser)

		list_index.assert_called_once_with()
		model_parser.get_node_list.assert_called_once_with()
		self.assertEqual(indexes, [
			{"label": "sample", "property": "guid"},
			{"label": "participant", "property": "guid"},
			{"label": "study", "property": "guid"},
		])
		self.assertEqual(session.run.call_args_list, [
			call("CREATE INDEX ON :participant(guid);"),
			call("CREATE INDEX ON :study(guid);"),
		])

	def test_create_index_uses_custom_id_field(self) -> None:
		session = MagicMock()
		loader = self._build_loader_with_mock_session(session)
		model_parser = MagicMock()
		model_parser.get_node_list.return_value = ["sample"]
		with patch.object(loader, "_list_index", return_value=[]):
			indexes = loader.create_index(model_parser, id_field="id")

		self.assertEqual(indexes, [{"label": "sample", "property": "id"}])
		session.run.assert_called_once_with("CREATE INDEX ON :sample(id);")

	def test_create_index_propagates_database_error(self) -> None:
		session = MagicMock()
		session.run.side_effect = RuntimeError("Index creation failed")
		loader = self._build_loader_with_mock_session(session)
		model_parser = MagicMock()
		model_parser.get_node_list.return_value = ["sample"]
		with patch.object(loader, "_list_index", return_value=[]):
			with self.assertRaisesRegex(RuntimeError, "Index creation failed"):
				loader.create_index(model_parser)

		session.run.assert_called_once_with("CREATE INDEX ON :sample(guid);")

	def test_drop_index_runs_query_for_each_index(self) -> None:
		session = MagicMock()
		loader = self._build_loader_with_mock_session(session)

		result = loader.drop_index([
			{"label": "sample", "property": "guid"},
			{"label": "participant", "property": "id"},
		])

		self.assertIsNone(result)
		self.assertEqual(session.run.call_args_list, [
			call("DROP INDEX ON :sample(guid);"),
			call("DROP INDEX ON :participant(id);"),
		])

	def test_drop_index_with_empty_list_does_not_run_queries(self) -> None:
		session = MagicMock()
		loader = self._build_loader_with_mock_session(session)

		self.assertIsNone(loader.drop_index([]))
		session.run.assert_not_called()

	def test_drop_index_propagates_database_error(self) -> None:
		session = MagicMock()
		session.run.side_effect = RuntimeError("Index removal failed")
		loader = self._build_loader_with_mock_session(session)

		with self.assertRaisesRegex(RuntimeError, "Index removal failed"):
			loader.drop_index([{"label": "sample", "property": "guid"}])

		session.run.assert_called_once_with("DROP INDEX ON :sample(guid);")

	def test_drop_all_indexes_calls_list_and_drop_index(self) -> None:
		loader = self._build_loader_with_mock_session(MagicMock())
		indexes = [
			{"label": "sample", "property": "guid"},
			{"label": "participant", "property": "id"},
		]
		with patch.object(loader, "_list_index", return_value=indexes) as list_index, \
				patch.object(loader, "drop_index") as drop_index:
			result = loader.drop_all_indexes()

		self.assertIsNone(result)
		list_index.assert_called_once_with()
		drop_index.assert_called_once_with(indexes)

	def test_wipe_database_repeats_batches_until_empty_and_sums_counts(self) -> None:
		session = MagicMock()
		results = [MagicMock() for _ in range(3)]
		for result, count in zip(results, [2, 1, 0]):
			result.single.return_value = {"deleted_count": count}
		session.run.side_effect = results
		loader = self._build_loader_with_mock_session(session)

		self.assertEqual(loader.wipe_database(batch_size=2), 3)

		self.assertEqual(session.run.call_count, 3)
		for query_call in session.run.call_args_list:
			self.assertEqual(query_call.kwargs, {"batch_size": 2})
			self.assertIn("WITH n LIMIT $batch_size", query_call.args[0])
			self.assertIn("DETACH DELETE n", query_call.args[0])

	def test_wipe_database_returns_zero_for_empty_database(self) -> None:
		session = MagicMock()
		session.run.return_value.single.return_value = {"deleted_count": 0}
		loader = self._build_loader_with_mock_session(session)

		self.assertEqual(loader.wipe_database(), 0)
		session.run.assert_called_once()
		self.assertEqual(session.run.call_args.kwargs, {"batch_size": 10000})

	def test_wipe_database_propagates_error_after_completed_batch(self) -> None:
		session = MagicMock()
		first_result = MagicMock()
		first_result.single.return_value = {"deleted_count": 2}
		session.run.side_effect = [first_result, RuntimeError("Wipe failed")]
		loader = self._build_loader_with_mock_session(session)

		with self.assertRaisesRegex(RuntimeError, "Wipe failed"):
			loader.wipe_database(batch_size=2)

		self.assertEqual(session.run.call_count, 2)

	def test_wipe_rooted_subgraph_without_nodes_does_not_delete(self) -> None:
		loader = self._build_loader_with_mock_session(MagicMock())
		with (
			patch.object(loader, "_get_rooted_subgraph_nodes_ids", return_value=[]) as get_ids,
			patch.object(loader, "_detach_nodes_by_ids") as detach,
			patch.object(loader, "_delete_nodes_by_ids") as delete,
		):
			summary = loader.wipe_rooted_subgraph("study", "study_id", "study-1")

		self.assertEqual(summary, {"nodes_deleted": 0, "relationships_detached": 0})
		get_ids.assert_called_once_with("study", "study_id", "study-1")
		detach.assert_not_called()
		delete.assert_not_called()

	def test_wipe_rooted_subgraph_at_or_below_threshold_detaches_before_deleting(self) -> None:
		for node_ids in ([1], [1, 2]):
			with self.subTest(node_ids=node_ids):
				loader = self._build_loader_with_mock_session(MagicMock())
				operations = MagicMock()
				with (
					patch.object(loader, "_get_rooted_subgraph_nodes_ids", return_value=node_ids),
					patch.object(loader, "_detach_nodes_by_ids", return_value=3) as detach,
					patch.object(loader, "_delete_nodes_by_ids", return_value=len(node_ids)) as delete,
				):
					operations.attach_mock(detach, "detach")
					operations.attach_mock(delete, "delete")
					summary = loader.wipe_rooted_subgraph(
						"study", "study_id", "study-1", chunk_threshold=2
					)

				self.assertEqual(summary, {
					"nodes_deleted": len(node_ids),
					"relationships_detached": 3,
				})
				self.assertEqual(operations.mock_calls, [
					call.detach(node_ids=node_ids),
					call.delete(node_ids=node_ids),
				])

	def test_wipe_rooted_subgraph_batches_all_detaches_before_deletions(self) -> None:
		loader = self._build_loader_with_mock_session(MagicMock())
		node_ids = list(range(5001))
		operations = MagicMock()
		with (
			patch.object(loader, "_get_rooted_subgraph_nodes_ids", return_value=node_ids) as get_ids,
			patch.object(loader, "_detach_nodes_by_ids", side_effect=[6000, 2]) as detach,
			patch.object(loader, "_delete_nodes_by_ids", side_effect=[5000, 1]) as delete,
		):
			operations.attach_mock(detach, "detach")
			operations.attach_mock(delete, "delete")
			summary = loader.wipe_rooted_subgraph(
				"study", "dbgap_accession", "phs000001", chunk_threshold=5000
			)

		get_ids.assert_called_once_with("study", "dbgap_accession", "phs000001")
		self.assertEqual(summary, {"nodes_deleted": 5001, "relationships_detached": 6002})
		self.assertEqual(operations.mock_calls, [
			call.detach(node_ids=node_ids[:5000]),
			call.detach(node_ids=node_ids[5000:]),
			call.delete(node_ids=node_ids[:5000]),
			call.delete(node_ids=node_ids[5000:]),
		])

	def test_wipe_rooted_subgraph_does_not_delete_nodes_after_detach_error(self) -> None:
		loader = self._build_loader_with_mock_session(MagicMock())
		with (
			patch.object(loader, "_get_rooted_subgraph_nodes_ids", return_value=[1, 2]),
			patch.object(loader, "_detach_nodes_by_ids", side_effect=RuntimeError("Detach failed")),
			patch.object(loader, "_delete_nodes_by_ids") as delete,
		):
			with self.assertRaisesRegex(RuntimeError, "Detach failed"):
				loader.wipe_rooted_subgraph("study", "study_id", "study-1")

		delete.assert_not_called()

	def test_get_rooted_subgraph_nodes_ids_passes_study_filter(self) -> None:
		for record, expected in (
			({"node_ids": [1, 2, 3]}, [1, 2, 3]),
			({"node_ids": []}, []),
			(None, []),
		):
			with self.subTest(record=record):
				session = MagicMock()
				session.run.return_value.single.return_value = record
				loader = self._build_loader_with_mock_session(session)

				node_ids = loader._get_rooted_subgraph_nodes_ids(
					"study", "dbgap_accession", "phs000001"
				)

				self.assertEqual(node_ids, expected)
				session.run.assert_called_once()
				query_call = session.run.call_args
				self.assertIn(
					"MATCH (s:study {dbgap_accession: $subgraph_value})",
					query_call.args[0],
				)
				self.assertIn("COLLECT(DISTINCT id(s))", query_call.args[0])
				self.assertIn("COLLECT(DISTINCT id(n))", query_call.args[0])
				self.assertEqual(query_call.kwargs, {"subgraph_value": "phs000001"})

	def test_detach_nodes_by_ids_passes_ids_and_returns_relationship_count(self) -> None:
		for record, expected in (({"deleted_rels": 4}, 4), (None, 0)):
			with self.subTest(record=record):
				session = MagicMock()
				session.run.return_value.single.return_value = record
				loader = self._build_loader_with_mock_session(session)

				self.assertEqual(loader._detach_nodes_by_ids([1, 2]), expected)

				session.run.assert_called_once()
				query_call = session.run.call_args
				self.assertEqual(query_call.kwargs, {"node_ids": [1, 2]})
				self.assertIn("WITH DISTINCT r", query_call.args[0])
				self.assertIn("DELETE r", query_call.args[0])

	def test_delete_nodes_by_ids_passes_ids_and_returns_node_count(self) -> None:
		for record, expected in (({"deleted_nodes": 2}, 2), (None, 0)):
			with self.subTest(record=record):
				session = MagicMock()
				session.run.return_value.single.return_value = record
				loader = self._build_loader_with_mock_session(session)

				self.assertEqual(loader._delete_nodes_by_ids([1, 2]), expected)

				session.run.assert_called_once()
				query_call = session.run.call_args
				self.assertEqual(query_call.kwargs, {"node_ids": [1, 2]})
				self.assertIn("WITH DISTINCT n", query_call.args[0])
				self.assertIn("DELETE n", query_call.args[0])

	def test_subgraph_database_helpers_propagate_database_errors(self) -> None:
		for method_name, args in (
			("_get_rooted_subgraph_nodes_ids", ("study", "study_id", "study-1")),
			("_detach_nodes_by_ids", ([1, 2],)),
			("_delete_nodes_by_ids", ([1, 2],)),
		):
			with self.subTest(method=method_name):
				session = MagicMock()
				session.run.side_effect = RuntimeError("Database unavailable")
				loader = self._build_loader_with_mock_session(session)

				with self.assertRaisesRegex(RuntimeError, "Database unavailable"):
					getattr(loader, method_name)(*args)

				session.run.assert_called_once()

	def test_check_unique_nodes_batch_returns_results_for_each_value(self) -> None:
		session = MagicMock()
		node_a = self._FakeGraphNode(labels={"sample"}, properties={"guid": "g1", "name": "A"})
		node_b = self._FakeGraphNode(labels={"sample"}, properties={"guid": "g2", "name": "B"})
		label_result = MagicMock()
		label_result.__iter__.return_value = iter([{"labels": ["sample"]}])
		unique_result = MagicMock()
		unique_result.__iter__.return_value = iter([
			{"property_value": "g1", "node": node_a},
			{"property_value": "g2", "node": node_b},
			{"property_value": "missing", "node": None},
		])

		def run_side_effect(query, **kwargs):
			if "RETURN DISTINCT labels(n)" in query:
				return label_result
			self.assertEqual(kwargs["property_values"], ["g1", "g2", "missing"])
			return unique_result

		session.run.side_effect = run_side_effect
		loader = self._build_loader_with_mock_session(session)

		results = loader.check_unique_nodes(
			property_values=["g1", "g2", "missing"],
			property_name="guid",
		)

		self.assertTrue(results["g1"][0])
		self.assertEqual(results["g1"][1]["check_result"], "Pass")
		self.assertTrue(results["g2"][0])
		self.assertFalse(results["missing"][0])
		self.assertEqual(results["missing"][1]["matched_node(s)"], [])

	def test_find_upstream_nodes_batch_returns_upstream_nodes_per_target(self) -> None:
		session = MagicMock()
		label_result = MagicMock()
		label_result.__iter__.return_value = iter([{"labels": ["sample"]}])
		upstream_result = MagicMock()
		upstream_result.__iter__.return_value = iter([
			{"property_value": "target-guid", "labels": ["participant"], "properties": {"guid": "p1", "name": "P1"}},
			{"property_value": "target-guid", "labels": ["study"], "properties": {"guid": "st1", "name": "Study 1"}},
			{"property_value": "other-guid", "labels": ["study"], "properties": {"guid": "st2", "name": "Study 2"}},
		])

		def run_side_effect(query, **kwargs):
			if "RETURN DISTINCT labels(n)" in query:
				return label_result
			self.assertEqual(kwargs["property_values"], ["target-guid", "other-guid"])
			return upstream_result

		session.run.side_effect = run_side_effect
		loader = self._build_loader_with_mock_session(session)

		results = loader.find_upstream_nodes_batch(
			property_values=["target-guid", "other-guid"],
			property_name="guid",
		)

		self.assertSetEqual(
			{node["properties"]["guid"] for node in results["target-guid"]},
			{"p1", "st1"},
		)
		self.assertEqual(results["other-guid"][0]["labels"], ["study"])

	def test_if_alternative_path_to_root_batch_returns_nested_bool_map(self) -> None:
		session = MagicMock()
		result = MagicMock()
		result.__iter__.return_value = iter([
			{"avoid": "avoid-guid", "target": "target-a", "has_path": True},
			{"avoid": "other-avoid", "target": "target-a", "has_path": False},
		])
		session.run.return_value = result
		loader = self._build_loader_with_mock_session(session)

		out = loader.if_alternative_path_to_root_batch(
			property_name="guid",
			avoid_to_targets_pairs=[
				{"avoid": "avoid-guid", "target": "target-a"},
				{"avoid": "avoid-guid", "target": "target-b"},
				{"avoid": "other-avoid", "target": "target-a"},
			],
			root_label="study",
		)

		self.assertTrue(out["avoid-guid"]["target-a"])
		self.assertFalse(out["avoid-guid"]["target-b"])
		self.assertFalse(out["other-avoid"]["target-a"])

	def test_if_multiple_outgoing_edges_batch_detects_multiple_outgoing_edges(self) -> None:
		session = MagicMock()
		result = MagicMock()
		result.__iter__.return_value = iter([
			{"property_value": "g1", "has_multiple_outgoing_edges": True},
			{"property_value": "g2", "has_multiple_outgoing_edges": False},
		])
		session.run.return_value = result
		loader = self._build_loader_with_mock_session(session)

		out = loader.if_multiple_outgoing_edges_batch(
			[
				{"label": "sample", "property_name": "guid", "property_value": "g1"},
				{"label": "sample", "property_name": "guid", "property_value": "g2"},
			]
		)

		self.assertTrue(out["g1"])
		self.assertFalse(out["g2"])
		session.run.assert_called_once()


if __name__ == "__main__":
	unittest.main()
