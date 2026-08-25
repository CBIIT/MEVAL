---
layout: default
title: Loader Workflows
permalink: /loader/workflows/
---
# Loader Workflows

MEVAL repo contains ready-to-deploy Prefect workflows. A handful of workflows below (in `workflows/prefect/`) instantiate and use a `meval.loader.Loader` instance to read/write the graph database. Each flow first retrieves database credentials from AWS Secrets Manager and creates a `neo4j.GraphDatabase.driver`, then wraps it with `Loader(driver=driver)` before calling one or more `Loader` methods.

## Contents
- [`upsert_workflow.py` — Dataloading Upsert Workflow](#upsert_workflowpy--dataloading-upsert-workflow)
- [`wipe_database.py` — Wipe out Database Flow](#wipe_databasepy--wipe-out-database-flow)
- [`delete_database_study_subgraph.py` — Delete Database Study Subgraph Flow](#delete_database_study_subgraphpy--delete-database-study-subgraph-flow)
- [`find_anchored_traversals.py` — Find Anchored Traversals Flow](#find_anchored_traversalspy--find-anchored-traversals-flow)
- [`precision_deletion_guid.py` — Precision Deletion Nodes](#precision_deletion_guidpy--precision-deletion-nodes)
- [`find_db_floating_nodes.py` — Find/delete floating (orphan) nodes](#find_db_floating_nodespy--finddelete-floating-orphan-nodes)

## `upsert_workflow.py` — Dataloading Upsert Workflow
Contains the two main data-loading flows, both of which create a `Loader` and use it to create indexes and upsert nodes/relationships:
- **`upsert_files`** — creates DB indexes (`Loader.create_index`), upserts node records then relationships for a single S3 TSV folder (`Loader.upsert_file_records` / `upsert_file_relationships`), and optionally calls `find_floating_db_nodes_flow` + `delete_nodes_by_internal_id_flow` afterward if `delete_floating_nodes_if_found` is set.
- **`upsert_files_in_order`** — same as `upsert_files`, but loads a **list** of S3 TSV folders sequentially, reusing the same `Loader`/driver across all folders.

## `wipe_database.py` — Wipe out Database Flow
Creates a `Loader` and calls `Loader.wipe_database()` to delete all nodes/relationships, followed by `Loader.drop_all_indexes()`. Destructive and irreversible — intended for non-production/test databases.

## `delete_database_study_subgraph.py` — Delete Database Study Subgraph Flow
Creates a `Loader` and, for each given root node property value (e.g. a list of study accessions), calls `Loader.wipe_rooted_subgraph(root_node_label, root_node_prop, subgraph_value)` to delete that entire rooted subgraph.

## `find_anchored_traversals.py` — Find Anchored Traversals Flow
Creates a `Loader` and, for each given intermediate-root node value (e.g. a list of `participant` IDs), calls `Loader.viz_intermediate_anchored_traversals(...)` to render and upload an HTML visualization of all descendant traversals within a larger rooted subgraph.

## `precision_deletion_guid.py` — Precision Deletion Nodes
Creates a `Loader` and calls `Loader.delete_nodes_by_prop_value(identifier_list, property_name)` to delete a specific set of nodes identified by UUID/`guid` values. Also invokes `find_floating_db_nodes_flow` (via `find_db_floating_nodes.py`) to report orphan nodes created as a side effect of the deletion, honoring the `dry_run` flag before any deletion occurs.

## `find_db_floating_nodes.py` — Find/delete floating (orphan) nodes
Defines two small sub-flows that take an existing `Loader` instance as a parameter (rather than creating their own driver), so they can be reused inside other flows:
- **`find_floating_db_nodes_flow(loader, output_filename, root_node_label="study")`** — streams nodes with no path to the given root node label via `Loader.find_nodes_without_path_to_root` and writes them to a JSON file.
- **`delete_nodes_by_internal_id_flow(loader, internal_ids_to_delete)`** — deletes nodes by internal database ID via `Loader.delete_nodes_by_internal_id`.

These two are called from both `upsert_workflow.py` (optional floating-node cleanup after loading) and `precision_deletion_guid.py` (reporting/cleanup after precision deletion), rather than being deployed as standalone entrypoints.
