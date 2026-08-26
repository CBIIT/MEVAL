---
layout: default
title: Loader Usage
permalink: /loader/usage/
---
# Loader Usage

`Loader` (`meval/loader.py`) handles ingestion of TSV data files into a Neo4j/Memgraph graph database, using MDF model metadata (via `ModelParser`) to interpret node types, property types, and relationships. It reads files in chunks, upserts nodes and relationships, manages indexes, and provides utilities for graph maintenance such as deleting subgraphs and finding orphan nodes.

## Contents

- [Instantiation](#instantiation)
- [Loading nodes and relationships](#loading-nodes-and-relationships)
- [Index management](#index-management)
- [Deleting records from database](#deleting-records-from-database)
- [Finding orphan / floating nodes](#finding-orphan--floating-nodes)
- [Traversal / graph inspection utilities](#traversal--graph-inspection-utilities)
- [Notes](#notes)

## Instantiation

```python
from neo4j import GraphDatabase
from meval.loader import Loader

driver = GraphDatabase.driver(uri, auth=(user, password))
loader = Loader(driver)

# close the underlying driver when the loading is done
loader.close()
```

## Loading nodes and relationships

### `upsert_file_records`
Upserts all node records from a TSV file into the database, in chunks (default `chunk_size=3000`). For each chunk, node properties are cleaned (whitespace trimmed, missing values dropped, list-type/number-type properties converted per the MDF model) and merged into the graph using the `id_field` (default `"guid"`) as the match key. Returns a combined summary dict of Neo4j counters (e.g. `nodes_created`, `properties_set`) across all chunks.

```python
summary = loader.upsert_file_records(
    file_path="participant.tsv",
    model_parser=model_parser, # a ModelParser instance
    subgraph_col="subgraph",   # optional column identifying the study/subgraph
    id_field="guid",
    chunk_size=3000,
)
```

### `upsert_file_relationships`
Upserts relationships declared by `<parent_node_type>.<parent_key_prop>` columns in a TSV file. Both the source and destination nodes must already exist (i.e. run after `upsert_file_records` for all related files). It also tracks which `guid` values have already been processed (via `processed_rel_dict`) within a loading job so that if the same `guid` reappears, previously established relationships for the old row are removed and replaced with the new one.

```python
processed_rel_dict = {}
summary, processed_rel_dict = loader.upsert_file_relationships(
    file_path="sample.tsv",
    model_parser=model_parser,
    processed_rel_dict=processed_rel_dict,
    id_field="guid",
    chunk_size=3000,
)
```

Rows with unmatched source/destination nodes are skipped (not upserted) and logged as `missing_src`/`missing_dst`/`missing_both`.

## Index management

- `create_index(model_parser, id_field="guid")` — creates an index on `id_field` for every node label defined in the model, skipping labels that already have one. Returns the list of indexes (existing + newly created).
- `drop_index(index_list)` — drops indexes given a list of `{"label": ..., "property": ...}` dicts.
- `drop_all_indexes()` — lists all existing indexes and drops them.

```python
created = loader.create_index(model_parser, id_field="guid")
loader.drop_index(created)
loader.drop_all_indexes()
```

## Deleting records from database

- `wipe_database(batch_size=10000)` — deletes all nodes/relationships in the database in batches, returns the total number of nodes deleted.
- `wipe_rooted_subgraph(root_node_label, root_node_prop, subgraph_value, chunk_threshold=10000)` — deletes an entire subgraph rooted at a node (e.g. a `study`) matched by `root_node_prop == subgraph_value`, including all descendant nodes and their relationships. Automatically batches deletion for large subgraphs (> `chunk_threshold` nodes). Returns `{"nodes_deleted": int, "relationships_detached": int}`.
- `delete_nodes_by_internal_id(identifier_list, batch_size=5000)` — deletes nodes by their internal database ID (`id(n)`), given as a list of numeric strings. Returns the number of nodes deleted.
- `delete_nodes_by_prop_value(identifier_list, property_name, batch_size=5000)` — deletes nodes whose `property_name` matches any value in `identifier_list` (typically a UUID/`guid` property). Returns the number of nodes deleted.

```python
loader.wipe_rooted_subgraph( # This can be used to delete a study or a program
    root_node_label="study",
    root_node_prop="study_id",
    subgraph_value="phs002790",
)

loader.delete_nodes_by_prop_value(
    identifier_list=["uuid1", "uuid2"],
    property_name="guid",
)
```

## Finding orphan / floating nodes

`find_nodes_without_path_to_root(root_node_label)` returns a generator of nodes that have no outgoing path to any node labeled `root_node_label` (e.g. `study`/`program`). These are orphan nodes disconnected from the root, or nodes whose path to the root is broken. Each yielded item includes `db_internal_id`, `type`, and `properties`.

```python
for orphan in loader.find_nodes_without_path_to_root(root_node_label="study"):
    print(orphan["type"], orphan["properties"])
```

## Traversal / graph inspection utilities

These helpers support inspecting a subgraph before performing a deletion, typically to identify all descendants of an "intermediate root" node (e.g. a `participant`) within a larger rooted subgraph (e.g. a `study`):

- `find_intermediate_anchored_descendants(...)` — returns a list of dicts (`ID`, `type`, `properties`) for the intermediate root node and all of its descendants.
- `export_intermediate_anchored_traversals(...)` — writes the traversal paths to a JSON file (with retry/backoff on transient Neo4j errors) and returns the output filename.
- `viz_intermediate_anchored_traversals(...)` — renders an interactive HTML visualization (via `neo4j_viz`) of the traversal paths and returns the output filename (skipped if the graph exceeds 1000 nodes).
- `check_unique_node(property_value, property_name="guid")` — checks whether exactly one node exists with a given property value; returns `(bool, details_dict)`.
- `find_upstream_nodes(property_value, property_name="guid")` — returns all nodes with an outgoing path to the node matched by `property_name`/`property_value`.
- `if_alternative_path_to_root(property_name, target_property_value, node_to_avoid_property_value, root_label)` — checks whether a target node has a path to a root-labeled node that does **not** pass through a given node (useful for verifying it's safe to delete a node without orphaning its target).

```python
# This example returns all descendants of a data node in the graph database
descendants = loader.find_intermediate_anchored_descendants(
    root_node_label="study",
    root_node_prop="study_id",
    root_node_prop_value="phs002790",
    intermediate_root_node_label="participant",
    intermediate_root_node_prop="guid",
    intermediate_root_node_prop_value="participant-uuid",
    model_parser=model_parser,
)
```

## Notes

- All instance methods that hit the database open their own `self.driver.session()`; the `Loader` instance itself only wraps a `driver` object.
- Methods prefixed with `_` (e.g. `_list_index`, `_get_rooted_subgraph_nodes_ids`, `_detach_nodes_by_ids`, `_delete_nodes_by_ids`) are internal helpers.
- Pass a `logging.Logger` via the `logger` parameter (where supported) to capture progress/errors in a log file in addition to stdout `print` output.